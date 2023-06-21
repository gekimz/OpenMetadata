import requests

from metadata.generated.schema.entity.services.connections.metadata.sasCatalogConnection import (
    SASCatalogConnection,
)
from metadata.ingestion.ometa.client import REST, APIError, ClientConfig
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class SASCatalogClient:
    """
    Client to interact with SAS Catalog
    """

    def __init__(self, config: SASCatalogConnection):
        self.config = config
        self.auth_token = get_token(
            config.serverHost, config.username, config.password.get_secret_value()
        )
        client_config: ClientConfig = ClientConfig(
            base_url=config.serverHost,
            auth_header="Authorization",
            auth_token=self.get_auth_token,
            api_version="",
            verify=False,
        )
        self.client = REST(client_config)

    def list_instances(self):
        # For now the entities we'll work with are tables
        logger.info("list_instances")
        cas_table_id = "3a0d5d7b-a1c5-44c6-bfad-0d2174236172"
        sas_table_id = "02b7102c-e997-465d-9f41-2491c3a4f05b"
        filter_state = f"filter=or(eq(definitionId,'{cas_table_id}'),eq(definitionId,'{sas_table_id}'))"
        endpoint = f"catalog/instances?{filter_state}&limit=2"
        response = self.client.get(endpoint)
        if "error" in response.keys():
            raise APIError(response["error"])
        return response["items"]

    def get_instance(self, instanceId):
        endpoint = f"catalog/instances/{instanceId}"
        response = self.client.get(endpoint)
        if "error" in response.keys():
            raise APIError(response["error"])
        return response

    def get_views(self, query):
        endpoint = "catalog/instances"
        headers = {
            "Content-type": "application/vnd.sas.metadata.instance.query+json",
            "Accept": "application/json",
        }
        logger.info(f"{query}")
        response = self.client._request(
            "POST", path=endpoint, data=query, headers=headers
        )
        if "error" in response.keys():
            raise APIError(f"{response}")
        logger.info("get_views success")
        return response

    def get_auth_token(self):
        return self.auth_token, 0


def get_token(baseURL, user, password):
    endpoint = "/SASLogon/oauth/token"
    payload = {"grant_type": "password", "username": user, "password": password}
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Authorization": "Basic c2FzLmNsaTo=",
    }
    url = baseURL + endpoint
    response = requests.request(
        "POST", url, headers=headers, data=payload, verify=False
    )
    text_response = response.json()
    logger.info(f"this is user: {user}, password: {password}, text: {text_response}")
    return response.json()["access_token"]
