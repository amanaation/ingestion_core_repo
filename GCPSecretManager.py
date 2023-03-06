import hashlib
from google.cloud import secretmanager


class SecretManager:

    def __init__(self, project_id):
        self.PROJECT_ID = project_id
        self.secretsmanagerclient = secretmanager.SecretManagerServiceClient()

    def access_secret(self, secret_id, version_id="latest"):
        name = f"projects/{self.PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
        response = self.secretsmanagerclient.access_secret_version(name=name)
        return response.payload.data.decode('UTF-8')

