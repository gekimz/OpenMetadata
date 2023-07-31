from typing import Optional

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.metadata.sasViyaConnection import (
    SASViyaConnection,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.metadata.sasviya.client import SASViyaClient
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


def get_connection(connection: SASViyaConnection) -> SASViyaClient:
    return SASViyaClient(connection)


def test_connection(
    metadata: OpenMetadata,
    client: SASViyaClient,
    service_connection: SASViyaConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    logger.info("before")
    test_fn = {"CheckAccess": client.list_instances}
    logger.info("after")
    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
    logger.info("afterafter")