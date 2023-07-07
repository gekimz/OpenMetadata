import json
import time
from typing import List

import jsonpatch

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.data.createTableProfile import (
    CreateTableProfileRequest,
)
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.api.services.createDashboardService import (
    CreateDashboardServiceRequest,
)
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.data.chart import Chart, ChartType
from metadata.generated.schema.entity.data.dashboard import Dashboard
from metadata.generated.schema.entity.data.table import (
    Column,
    ColumnProfile,
    CustomMetricProfile,
    Table,
    TableData,
    TableProfile,
)
from metadata.generated.schema.entity.services.connections.dashboard.customDashboardConnection import (
    CustomDashboardConnection,
    CustomDashboardType,
)
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
    CustomDatabaseType,
)
from metadata.generated.schema.entity.services.connections.database.mysqlConnection import (
    MysqlConnection,
    MySQLType,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.sasCatalogConnection import (
    SASCatalogConnection,
    SasCatalogType,
)
from metadata.generated.schema.entity.services.dashboardService import (
    DashboardConnection,
    DashboardServiceType,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.entity.services.metadataService import (
    MetadataConnection,
    MetadataService,
    MetadataServiceType,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import EntityExtension
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.source import InvalidSourceException, Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn
from metadata.ingestion.source.database.column_type_parser import ColumnTypeParser
from metadata.ingestion.source.metadata.sascatalog.client import SASCatalogClient
from metadata.ingestion.source.metadata.sascatalog.extension_attr import (
    TABLE_CUSTOM_ATTR,
)
from metadata.profiler.api.models import ProfilerResponse
from metadata.profiler.sink.metadata_rest import (
    MetadataRestSink,
    MetadataRestSinkConfig,
)
from metadata.utils import fqn
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
        self.test_connection()

        self.db_service_name = None
        self.db_name = None
        self.db_schema_name = None
        self.table_fqns = None

        self.dashboard_service_name = None

    # self.add_table_custom_attributes()

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
        # for table in table_entities:
        #    yield from self.create_table_entity(table)
        #'''
        report_entities = self.sasCatalog_client.list_reports()
        for report in report_entities:
            self.table_fqns = []
            # There isn't a schema for creating report entities, maybe this'll work instead
            report_id = report["id"]
            report_instance = self.sasCatalog_client.get_instance(report_id)
            report_resource = report_instance["resourceId"]
            report_resource_id = self.sasCatalog_client.get_resource(
                report_resource[1:]
            )["id"]
            report_charts = self.sasCatalog_client.get_visual_elements(
                report_resource_id
            )
            supported_chart_types = ["Graph", "Text", "Table"]
            filtered_charts = filter(lambda x: x["@element"] in supported_chart_types)
            for chart in filtered_charts:
                yield from self.create_chart_entity(chart)
            report_tables = self.get_report_tables(report_resource_id)
            for table in report_tables:
                yield from self.create_table_entity(table)
            yield from self.create_report_entity(report)

        #'''

    def create_database_service(self, service_name):
        # I should also probably add some functionality to check if a db_service, db, db_schema already exist
        # on Open Metadata's end

        # Create a custom database connection config
        # I wonder what will happen if you use this source class as the source python class
        # For custom database connections - we will provide client credentials via the connection options
        self.db_service_name = service_name
        db_service = CreateDatabaseServiceRequest(
            name=service_name,
            serviceType=DatabaseServiceType.CustomDatabase,
            connection=DatabaseConnection(
                config=CustomDatabaseConnection(
                    type=CustomDatabaseType.CustomDatabase,
                    sourcePythonClass="metadata.ingestion.source.database.customdatabase.metadata.SASCatalogDB",
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
        # data_store_endpoint = db["resourceId"][1:]
        # logger.info(f"{data_store_endpoint}")
        # data_store_resource = self.sasCatalog_client.get_data_source(
        #     data_store_endpoint
        # )
        # db_service = self.create_database_service(data_store_resource["providerId"])
        #
        # data_store_parent_endpoint = ""
        # for link in data_store_resource["links"]:
        #     if link["rel"] == "parent":
        #         data_store_parent_endpoint = link["uri"][1:]
        #         break
        #
        # data_store_parent = self.sasCatalog_client.get_data_source(
        #     data_store_parent_endpoint
        # )
        # self.db_name = data_store_parent["id"]
        # database = CreateDatabaseRequest(
        #     name=data_store_parent["id"],
        #     displayName=data_store_parent["name"],
        #     service=db_service.fullyQualifiedName,
        # )
        # database_entity = self.metadata.create_or_update(data=database)
        # return database_entity
        db_service = self.create_database_service(db["providerId"])
        data_store_parent_endpoint = ""
        for link in db["links"]:
            if link["rel"] == "parent":
                data_store_parent_endpoint = link["uri"][1:]
                break
        data_store_parent = self.sasCatalog_client.get_data_source(
            data_store_parent_endpoint
        )
        self.db_name = data_store_parent["id"]
        database = CreateDatabaseRequest(
            name=data_store_parent["id"],
            displayName=data_store_parent["name"],
            service=db_service.fullyQualifiedName,
        )
        database_entity = self.metadata.create_or_update(data=database)
        return database_entity

    def create_database_schema(self, table):
        table_detail = self.sasCatalog_client.get_instance(table["id"])
        # We find the "database" entity in catalog
        # We first see if the table is a member of the library through the relationships attribute
        # Or we could use views to query the dataStores
        # data_store_data_sets = "4b114f6e-1c2a-4060-9184-6809a612f27b"
        # data_store_id = None
        # for relation in table_detail["relationships"]:
        #     if relation["definitionId"] != data_store_data_sets:
        #         continue
        #     data_store_id = relation["endpointId"]
        #     break
        #
        # if data_store_id is None:
        #     # For now we'll print error since we are exclusively working with tables in dataTables
        #     logger.error("Data store id should not be none")
        #     return None
        #
        # data_store = self.sasCatalog_client.get_instance(data_store_id)
        # database = self.create_database(data_store)
        # self.db_schema_name = data_store["name"]
        # db_schema = CreateDatabaseSchemaRequest(
        #     name=data_store["name"], database=database.fullyQualifiedName
        # )
        # db_schema_entity = self.metadata.create_or_update(db_schema)
        # return db_schema_entity
        table_resource_endpoint = table_detail["resourceId"][1:]
        table_resource = self.sasCatalog_client.get_resource(table_resource_endpoint)
        data_store_endpoint = ""
        for link in table_resource["links"]:
            if link["rel"] == "dataSource":
                data_store_endpoint = link["uri"][1:]
                break
        data_store = self.sasCatalog_client.get_data_source(data_store_endpoint)
        database = self.create_database(data_store)
        self.db_schema_name = data_store["name"]
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
        table_resource_id = self.sasCatalog_client.get_instance(table_id)["resourceId"][
            1:
        ]
        views_query = {
            "query": "match (t:dataSet)-[r:dataSetDataFields]->(c:dataField) return t,r,c",
            "parameters": {"t": {"id": f"{table_id}"}},
        }
        views_data = json.dumps(views_query)
        views = self.sasCatalog_client.get_views(views_data)
        # views_obj = json.loads(views)
        entities = views["entities"]

        # For now many dataField attributes will be cut since currently there is no functionality for adding custom
        # attributes to columns - luckily this functionality exists for tables so dataSet fields will be included
        columns: List[Column] = []
        col_count = (
            0
            if "columnCount" not in table_extension
            else table_extension["columnCount"]
        )
        row_count = (
            0 if "rowCount" not in table_extension else table_extension["rowCount"]
        )
        counter = 0

        col_profile_list = []
        # Creating the columns of the table
        for entity in entities:
            if entity["id"] == table_id:
                continue
            if "Column" not in entity["type"]:
                continue
            counter += 1
            col_attributes = entity["attributes"]
            if "casDataType" in col_attributes:
                datatype = col_attributes["casDataType"]
            else:
                datatype = col_attributes["dataType"]
            if datatype == "num":
                datatype = "numeric"
            parsed_string = ColumnTypeParser._parse_datatype_string(datatype)
            col_name = entity["name"]
            parsed_string["name"] = col_name.replace('"', "'")
            parsed_string["ordinalPosition"] = col_attributes["ordinalPosition"]
            # Column profile to be added
            attr_map = {
                "mean": "mean",
                "median": "sum",
                "min": "min",
                "max": "max",
                "standardDeviation": "stddev",
                "missingCount": "nullCount",
                "completenessPercent": "valuesPercentage",
                "uniquenessPercent": "uniqueProportion",
                "cardinalityCount": "distinctCount",
                "skewness": "nonParametricSkew",
                "quantiles25": "firstQuartile",
                "quantiles50": "median",
                "quantiles75": "thirdQuartile",
                "mismatchedCount": "missingCount",
                "charsMinCount": "minLength",
                "charsMaxCount": "maxLength",
                # "rawLength": "valuesCount",
            }
            extra_metrics = [
                "nOutliers",
                "mode",
                "semanticTypeScore",
                "mostCommonValue",
                "leastCommonValue",
                "mismatchedCount",
                "nRowsPositiveSentiment",
                "nRowsNegativeSentiment",
                "nRowsNeutralSentiment",
                "pctRowsPositiveSentiment",
                "pctRowsNegativeSentiment",
                "pctRowsNeutralSentiment",
                "sentimentIDScore",
            ]
            col_profile_dict = dict()
            for attr in attr_map:
                if attr in col_attributes:
                    if attr == "uniquenessPercent":
                        col_profile_dict[attr_map[attr]] = col_attributes[attr] / 100
                    else:
                        col_profile_dict[attr_map[attr]] = col_attributes[attr]

            if "rowCount" in table_extension:
                col_profile_dict["valuesCount"] = table_extension["rowCount"]
            if "valuesCount" in col_profile_dict:
                if "distinctCount" in col_profile_dict:
                    col_profile_dict["distinctProportion"] = (
                        col_profile_dict["distinctCount"]
                        / col_profile_dict["valuesCount"]
                    )
                    col_profile_dict["uniqueCount"] = col_profile_dict["distinctCount"]
                if "nullCount" in col_profile_dict:
                    col_profile_dict["nullProportion"] = (
                        col_profile_dict["nullCount"] / col_profile_dict["valuesCount"]
                    )
                if "missingCount" in col_profile_dict:
                    col_profile_dict["missingPercentage"] = (
                        col_profile_dict["missingCount"]
                        / col_profile_dict["valuesCount"]
                    )
                    col_profile_dict["validCount"] = (
                        col_profile_dict["valuesCount"]
                        - col_profile_dict["missingCount"]
                    )

            custom_metrics_list: List[CustomMetricProfile] = []
            for metric in extra_metrics:
                if metric in col_attributes:
                    if (datatype != "numeric") ^ (
                        metric in ["mode", "mostCommonValue", "leastCommonValue"]
                    ):
                        custom_metrics = CustomMetricProfile(
                            name=metric, value=col_attributes[metric]
                        )
                        custom_metrics_list.append(custom_metrics)
            col_profile_dict["customMetricsProfile"] = custom_metrics_list
            timestamp = time.time()
            col_profile_dict["timestamp"] = timestamp
            col_profile_dict["name"] = parsed_string["name"]
            column_profile = ColumnProfile(**col_profile_dict)
            col_profile_list.append(column_profile)
            parsed_string["profile"] = column_profile

            if datatype in ["char", "varchar", "binary", "varbinary"]:
                if "charsMaxCount" in col_attributes:
                    parsed_string["dataLength"] = col_attributes["charsMaxCount"]
                else:
                    parsed_string["dataLength"] = 0
            logger.info(f"This is parsed string: {parsed_string}")
            col = Column(**parsed_string)
            columns.append(col)

        # assert counter == col_count
        logger.info(f"{table_extension}")
        # Building table extension attr
        table_ext_attr = EntityExtension(__root__=table_extension)

        for attr in table_extension:
            if type(table_extension[attr]) == bool:
                table_extension[attr] = str(table_extension[attr])

        table_request = CreateTableRequest(
            name=table_id,
            displayName=table_name,
            columns=columns,
            databaseSchema=database_schema.fullyQualifiedName,
            # extension=table_extension
            # extension=...,  # To be added
        )

        yield table_request

        table_profile_dict = dict()
        timestamp = time.time()
        table_profile_dict["timestamp"] = timestamp
        table_profile_dict["rowCount"] = row_count
        table_profile_dict["columnCount"] = col_count
        table_profile = TableProfile(**table_profile_dict)
        table_profile_request = CreateTableProfileRequest(
            tableProfile=table_profile, columnProfile=col_profile_list
        )

        table_fqn = fqn.build(
            self.metadata,
            entity_type=Table,
            service_name=self.db_service_name,
            database_name=self.db_name,
            schema_name=self.db_schema_name,
            table_name=table_id,
        )

        self.table_fqns.append(table_fqn)
        table_entity = self.metadata.get_by_name(entity=Table, fqn=table_fqn)
        patches = []
        for attr in table_extension:
            patch = {
                "op": "add",
                "path": "/extension",
                "value": {f"{attr}": f"{table_extension[attr]}"},
            }
            patches.append(patch)
        patch = [{"op": "add", "path": "/extension", "value": table_extension}]
        self.metadata.client.patch(
            path=f"/tables/{table_entity.id.__root__}", data=json.dumps(patch)
        )
        """
        for column in table_entity.columns:
            resp = self.metadata.client.get(
                path=f"/tables/{table_fqn}.{column.name.__root__}/tableProfile"
            )
            print(resp.text)
        """
        rows, cols = self.sasCatalog_client.get_rows_cols(table_resource_id)
        table_data = {"columns": cols, "rows": rows}
        self.metadata.client.put(
            path=f"{self.metadata.get_suffix(Table)}/{table_entity.id.__root__}/sampleData",
            data=json.dumps(table_data),
        )
        self.metadata.client.put(
            path=f"{self.metadata.get_suffix(Table)}/{table_entity.id.__root__}/tableProfile",
            data=table_profile_request.json(),
        )

        """
        # Building Profiler Response
        table_sample_data = TableData(columns=cols, rows=rows)
        table_profile = ProfilerResponse(
            table=table_entity,
            profile=table_profile_request,
            sample_data=table_sample_data,
        )
        sink = MetadataRestSink(MetadataRestSinkConfig(), self.metadata_config)
        sink.write_record(table_profile) """

    def add_table_custom_attributes(self):
        table_type = self.metadata.client.get(path="/metadata/types/name/table")
        table_id = table_type["id"]
        for attr in TABLE_CUSTOM_ATTR:
            self.metadata.client.put(
                path=f"/metadata/types/{table_id}", data=json.dumps(attr)
            )

    def update_table_custom_attributes(self):
        pass

    def create_sample_data(self, table_id):
        rows_source, col_names = self.sasCatalog_client.get_rows_cols(table_id)
        rows = list(map(lambda x: x["cells"], rows_source))
        return TableData(columns=col_names, rows=rows)

    def create_dashboard_service(self):
        dashboard_service_name = "SAS_reports"
        self.dashboard_service_name = dashboard_service_name
        dashboard_service_request = CreateDashboardServiceRequest(
            name=dashboard_service_name,
            serviceType=DashboardServiceType.CustomDashboard,
            connection=DashboardConnection(
                config=CustomDashboardConnection(
                    type=CustomDashboardType.CustomDashboard,
                    sourcePythonClass="metadata.ingestion.source.database.customdatabase.metadata.SASCatalogDB",
                )
            ),
        )
        dashboard_service_entity = self.metadata.create_or_update(
            dashboard_service_request
        )
        return dashboard_service_entity

    def create_chart_entity(self, chart):
        chart_type_map = {
            "Text": ChartType.Text,
            "Table": ChartType.Table,
            "timeSeries": ChartType.Line,
            "Graph": {
                "bar": ChartType.Bar,
                "keyValue": ChartType.Table,
                "pie": ChartType.Pie,
            },
        }
        chart_type = chart_type_map["@element"]
        chart_request = CreateChartRequest(
            name=chart["name"],
            displayName=chart["labelAttribute"],
            chartType=chart_type,
            service=self.dashboard_service_name,
        )
        yield chart_request

    def get_report_tables(self, report_id):
        report_tables = self.sasCatalog_client.get_report_relationship(report_id)
        table_instances = []
        for table in report_tables:
            table_uri = table["relatedResourceUri"][1:]
            table_resource = self.sasCatalog_client.get_resource(table_uri)
            table_name = table_resource["name"]
            table_data_resource = table_resource["tableReference"]["tableUri"]
            param = f"filter=and(eq(name, '{table_name}'),eq(resourceId,'{table_data_resource}'))"
            table_instance = self.sasCatalog_client.get_instances_with_param(param)[0]
            table_instances.append(table_instance)
        return table_instances

        #     yield from self.create_table_entity(table_instance)
        #     table_entity = self.metadata.get_by_name(entity=Table, fqn=self.table_fqn)
        #     table_entities.append(table_entity)
        # return table_entities

    def create_lineage_request(self, dashboard, table_entities):
        for table in table_entities:
            return AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=EntityReference(id=table.id.__root__, type="table"),
                    toEntity=EntityReference(
                        id=dashboard.id.__root__, type="dashboard"
                    ),
                )
            )

    def create_report_entity(self, report):
        report_id = report["id"]
        report_instance = self.sasCatalog_client.get_instance(report_id)
        logger.info(f"{self.config.type}")
        logger.info(f"{self.service_connection}")
        dashboard_service = self.create_dashboard_service()
        report_resource = report_instance["resourceId"]
        report_url = self.sasCatalog_client.get_report_link(report_resource)
        report_request = CreateDashboardRequest(
            name=report_id,
            displayName=report_instance["name"],
            dashboardUrl=report_url,
            service=dashboard_service.fullyQualifiedName,
        )
        yield report_request

        dashboard_fqn = fqn.build(
            self.metadata,
            entity_type=Dashboard,
            service_name=self.dashboard_service_name,
            dashboard_name=report_id,
        )

        dashboard_entity = self.metadata.get_by_name(
            entity=Dashboard, fqn=dashboard_fqn
        )
        table_entities = []
        for table in self.table_fqns:
            table_entity = self.metadata.get_by_name(entity=Table, fqn=table)
            table_entities.append(table_entity)
        yield self.create_lineage_request(dashboard_entity, table_entities)

    def close(self):
        pass

    def test_connection(self) -> None:
        test_connection_fn = get_test_connection_fn(self.service_connection)
        test_connection_fn(self.metadata, self.connection_obj, self.service_connection)
