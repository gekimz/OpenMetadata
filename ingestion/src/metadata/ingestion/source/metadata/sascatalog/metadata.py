import json
from typing import List

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.data.table import Column
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
    CustomDatabaseType,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.sasCatalogConnection import (
    SASCatalogConnection,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.source import InvalidSourceException, Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn
from metadata.ingestion.source.database.column_type_parser import ColumnTypeParser
from metadata.ingestion.source.metadata.sascatalog.client import SASCatalogClient
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class SascatalogSource(Source):
    config: WorkflowSource
    sasCatalog_client: SASCatalogClient

    def __init__(
        self,
        config: WorkflowSource,
        metadata_config: OpenMetadataConnection,
    ):
        super().__init__()
        self.config = config
        self.metadata_config = metadata_config
        self.metadata = OpenMetadata(metadata_config)
        self.service_connection = self.config.serviceConnection.__root__.config

        self.sasCatalog_client = get_connection(self.service_connection)
        self.connection_obj = self.sasCatalog_client
        logger.info("init source")
        test_result = self.sasCatalog_client.list_instances()
        logger.info("after source")
        test_table = self.create_table_entity(test_result[0])
        logger.info(f"success {test_table}")
        self.test_connection()

    @classmethod
    def create(cls, config_dict, metadata_config: OpenMetadataConnection):
        logger.info(f"running create {config_dict}")
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: SASCatalogConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, SASCatalogConnection):
            raise InvalidSourceException(
                f"Expected SASCatalogConnection, but got {connection}"
            )
        return cls(config, metadata_config)

    def prepare(self):
        pass

    def next_record(self):
        table_entities = self.sasCatalog_client.list_instances()
        for table in table_entities:
            yield from self.create_table_entity(table)

    def create_database_service(self, service_name):
        # I should also probably add some functionality to check if a db_service, db, db_schema already exist
        # on Open Metadata's end

        # Create a custom database connection config
        # I wonder what will happen if you use this source class as the source python class
        db_service = CreateDatabaseServiceRequest(
            name=service_name,
            serviceType=DatabaseServiceType.CustomDatabase,
            connection=DatabaseConnection(
                config=CustomDatabaseConnection(
                    type=CustomDatabaseType.CustomDatabase,
                    sourcePythonClass="metadata.SascatalogSource",
                )
            ),
        )
        db_service_entity = self.metadata.create_or_update(data=db_service)
        if db_service_entity is None:
            logger.error(f"Create a service with name {service_name}")
        return db_service_entity

    def create_database(self, db):
        # We find the name of the mock DB service
        # Use the link to the parent of the resourceId of the datastore itself, and use its name
        # Then the db service name will be the provider id
        data_store_endpoint = db["resourceURI"][1:]
        data_store_resource = self.sasCatalog_client.get_data_source(
            data_store_endpoint
        )
        db_service = self.create_database_service(data_store_resource["providerId"])

        data_store_parent_endpoint = ""
        for link in data_store_resource["links"]:
            if link["rel"] == "parent":
                data_store_parent_endpoint = link["uri"][1:]
                break

        data_store_parent = self.sasCatalog_client.get_data_source(
            data_store_parent_endpoint
        )
        database = CreateDatabaseRequest(
            name=data_store_parent["id"],
            displayName=data_store_parent["name"],
            service=db_service,
        )
        database_entity = self.metadata.create_or_update(data=database)
        return database_entity

    def create_database_schema(self, table):
        table_detail = self.sasCatalog_client.get_instance(table["id"])
        # We find the "database" entity in catalog
        # We first see if the table is a member of the library through the relationships attribute
        # Or we could use views to query the dataStores
        data_store_data_sets = "4b114f6e-1c2a-4060-9184-6809a612f27b"
        data_store_id = None
        for relation in table_detail["relationships"]:
            if relation["definitionId"] != data_store_data_sets:
                continue
            data_store_id = relation["endpointId"]
            break

        if data_store_id is None:
            # For now we'll print error since we are exclusively working with tables in dataTables
            logger.error("Data store id should not be none")
            return None

        data_store = self.sasCatalog_client.get_instance(data_store_id)
        database = self.create_database(data_store)
        db_schema = CreateDatabaseSchemaRequest(
            name=data_store["name"], database=database.fullyQualifiedName
        )
        db_schema_entity = self.metadata.create_or_update(db_schema)
        return db_schema_entity

    def create_table_entity(self, table):
        # Create database + db service
        # Create database schema
        database_schema = self.create_database_schema(table)
        table_id = table["id"]
        table_name = table["name"]
        table_extension = table["attributes"]
        views_query = {
            "query": "match (t:dataSet)-[r:dataSetDataFields]->(c:dataField) return t,r,c",
            "parameters": {"t": {"id": f"{table_id}"}},
        }
        views_data = json.dumps(views_query)
        views = self.sasCatalog_client.get_views(views_data)
        views_obj = json.loads(views)
        entities = views_obj["entities"]

        # For now many dataField attributes will be cut since currently there is no functionality for adding custom
        # attributes to columns - luckily this functionality exists for tables so dataSet fields will be included
        columns: List[Column] = []
        col_count = (
            0
            if "columnCount" not in table_extension
            else table_extension["columnCount"]
        )
        counter = 0

        # Creating the columns of the table
        for entity in entities:
            if entity["id"] == table_id:
                continue
            if "Column" not in entity["type"]:
                continue
            counter += 1
            col_attributes = entity["attributes"]
            datatype = col_attributes["casDataType"]
            parsed_string = ColumnTypeParser._parse_datatype_string(datatype)
            parsed_string["name"] = entity["name"]
            # Column profile to be added
            col = Column(**parsed_string)
            columns.append(col)

        assert counter == col_count

        table_request = CreateTableRequest(
            name=table_id,
            displayName=table_name,
            columns=columns,
            databaseSchema=database_schema
            # extension=...,  # To be added
        )

        yield table_request

    def close(self):
        pass

    def test_connection(self) -> None:
        test_connection_fn = get_test_connection_fn(self.service_connection)
        test_connection_fn(self.metadata, self.connection_obj, self.service_connection)
