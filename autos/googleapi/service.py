import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Follow authentication steps here: https://developers.google.com/drive/api/v3/quickstart/python

from .errors import ServiceNotInitialized


class Service:
    def __init__(self, scopes, api_name, api_version):
        self.scopes = scopes
        self.api_name = api_name
        self.api_version = api_version
        self._service = None

    @property
    def service(self):
        if self._service is not None:
            return self._service
        raise ServiceNotInitialized('You need to call init_service() to initialize service.')

    def _get_credentials(self, credentials_file, client_secrets_file):
        """Gets cached Google credentials, or generates a new one.

        :type credentials_file: string
        :param credentials_file: Path to cached credentials file.

        :type client_secrets_file: string
        :param client_secrets_file: Path to client secrets file.
        """

        credentials = None

        # The credentials_file stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(credentials_file):
            with open(credentials_file, 'rb') as token:
                credentials = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, self.scopes)
                credentials = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(credentials_file, 'wb') as token:
                pickle.dump(credentials, token)
        return credentials

    def init_service(self, credentials_file, client_secrets_file, api_key):
        """Initializes Google API authenticated service.

        :type credentials_file: string
        :param credentials_file: Path to cached credentials file.

        :type client_secrets_file: string
        :param client_secrets_file: Path to client secrets file.

        :type api_key: string
        :param api_key: https://github.com/googleapis/google-api-python-client/blob/master/docs/api-keys.md
        """

        credentials = self._get_credentials(credentials_file, client_secrets_file)
        self._service = build(self.api_name, self.api_version, credentials=credentials, developerKey=api_key)
        return self
