from unittest import TestCase

from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.ingestion.source.metadata.sasviya.client import SASViyaClient
from metadata.ingestion.source.metadata.sasviya.metadata import SasviyaSource

mock_sasviya_config = {
    "source": {
        "type": "sasviya",
        "serviceName": "local_sasviya",
        "serviceConnection": {
            "config": {
                "type": "SASViya",
                "username": "username",
                "password": "password",
                "serverHost": "serverHost",
            }
        },
        "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "openMetadataServerConfig": {
            "hostPort": "http://localhost:8585/api",
            "authProvider": "openmetadata",
            "securityConfig": {
                "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGc"
                "iOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE"
                "2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXB"
                "iEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fN"
                "r3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3u"
                "d-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
            },
        }
    },
}

# Mock tables from SAS Viya
"""
Required attributes:
id : string
name : string
attributes: dict
resourceId : string

"""
MOCK_TABLES = [{"links": {{"rel": "dataSource", "uri": "..."}}}]
MOCK_DATA_STORES = [
    {
        "name": "data_store",
        "provierId": "provider",
        "links": {{"rel": "parent", "uri": "/parent"}},
    }
]
MOCK_PARENT_DATA_STORES = [{"id": "parent_data_store_0", "name": "parent_data_store"}]
MOCK_REPORTS = []
EXPECTED_RESULTS = []


class SasviyaUnitTest(TestCase):
    @patch(
        "metadata.ingestion.source.metadata.sasviya.metadata.SasviyaSource.test_connection"
    )
    def __init__(self, methodName, test_connection) -> None:
        super().__init__(methodName)
        test_connection.return_value = False
        self.config = OpenMetadataWorkflowConfig.parse_obj(mock_sasviya_config)
        self.sasviya = SasviyaSource.create(
            mock_sasviya_config["source"],
            self.config.workflowConfig.openMetadataServerConfig,
        )

    def mock_list_reports(self):
        return MOCK_REPORTS

    def mock_get_instance(self, id):
        for table in MOCK_TABLES:
            if table["id"] == id:
                return table
        for report in MOCK_REPORTS:
            if report["id"] == id:
                return report

    def mock_get_views(self, query):
        return {"entities": {}}

    def mock_get_resource(self, endpoint):
        # This is for the mock table
        return ()

    def mock_get_data_source(self, endpoint):
        # This is for the mock data store
        # Add conditionals for specific id/names of resources using the endpoint being passed
        pass

    # For the parent data store

    @patch.object(SASViyaClient, "list_reports", mock_list_reports)
    @patch.object(SASViyaClient, "get_instance", mock_get_instance)
    @patch.object(SASViyaClient, "get_views", mock_get_views)
    @patch.object(SASViyaClient, "get_resource", mock_get_resource)
    @patch.object(SASViyaClient, "get_data_source", mock_get_data_source)
    def test_lineage(self):
        pass
