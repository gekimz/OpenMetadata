import requests

from metadata.generated.schema.entity.services.connections.metadata.sasCatalogConnection import (
    SASCatalogConnection,
)
from metadata.ingestion.ometa.client import REST, APIError, ClientConfig


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
        )
        self.client = REST(client_config)

    def list_instances(self):
        # For now the entities we'll work with are tables
        endpoint = "/catalog/instances?filter=contains(name,'Table')"
        response = self.client.get(endpoint)
        if "error" in response.keys():
            raise APIError(response["error"])
        return response["items"]

    def get_instance(self, instanceId):
        endpoint = f"/catalog/instances/{instanceId}"
        response = self.client.get(endpoint)
        if "error" in response.keys():
            raise APIError(response["error"])
        return response

    def get_auth_token(self):
        return self.auth_token


def get_token(baseURL, user, password):
    endpoint = "/SASLogon/oauth/token"
    payload = {"grant_type": "password", "username": user, "password": password}
    headers = {
        "Content-type": "application/x-www-from-urlencoded",
        "Authorization": "c2FzLmNsaTo=",
    }
    url = baseURL + endpoint
    response = request.request("POST", url, header=headers, data=payload, verify=False)
    return response.json()["access_token"]
