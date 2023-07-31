from typing import Optional

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.sasViyaConnection import (
    SASViyaConnection,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.metadata.sasviya.client import SASViyaClient


def get_connection(connection: CustomDatabaseConnection) -> SASViyaClient:
    sas_connection = SASViyaConnection(
        username=connection.connectionOptions.__root__.get("username"),
        password=connection.connectionOptions.__root__.get("password"),
        serverHost=connection.connectionOptions.__root__.get("serverHost"),
    )
    return SASViyaClient(sas_connection)


def test_connection(
    metadata: OpenMetadata,
    client: SASViyaClient,
    service_connection: CustomDatabaseConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    test_fn = {"CheckAccess": client.list_instances}
    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )