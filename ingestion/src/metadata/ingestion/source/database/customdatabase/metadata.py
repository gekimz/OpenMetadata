from typing import Iterable

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.sasCatalogConnection import (
    SASCatalogConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.source import InvalidSourceException, Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn
from metadata.ingestion.source.metadata.sascatalog.client import SASCatalogClient


class SASCatalogDB(Source):
    def __init__(self, config: WorkflowSource, metadata_config: OpenMetadataConnection):
        super().__init__()
        self.config = config
        self.metadata_config = metadata_config
        self.metadata = OpenMetadata(metadata_config)
        self.service_connection = self.config.serviceConnection.__root__.config

        self.sas_connection = SASCatalogConnection(
            username=self.service_connection.connectionOptions.__root__.get("username"),
            password=self.service_connection.connectionOptions.__root__.get("password"),
            serverHost=self.service_connection.connectionOptions.__root__.get(
                "serverHost"
            ),
        )

        self.sas_catalog_client = get_connection(self.sas_connection)
        self.test_connection()

    @classmethod
    def create(
        cls, config_dict: dict, metadata_config: OpenMetadataConnection
    ) -> "Source":
        pass

    def prepare(self):
        pass

    def next_record(self) -> Iterable[Entity]:
        pass

    def test_connection(self) -> None:
        pass

    def close(self):
        pass
