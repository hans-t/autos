import httplib2
from apiclient.discovery import build
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets

from .errors import ServiceNotInitialized


class Service:
    def __init__(self, scope, api_name, api_version):
        self.scope = scope
        self.api_name = api_name
        self.api_version = api_version
        self._service = None

    @property
    def service(self):
        if self._service is not None:
            return self._service
        raise ServiceNotInitialized('You need to call init_service() to initialize service.')

    def init_service(self, credentials_file, client_secrets_file):
        """Initialize Google API authenticated service.

        :type credentials_file: string
        :param credentials_file: Path to cached credentials file.

        :type client_secrets_file: string
        :param client_secrets_file: Path to client secrets file.
        """

        storage = Storage(credentials_file)
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            flow = flow_from_clientsecrets(client_secrets_file, scope=self.scope)
            credentials = tools.run_flow(flow, storage, tools.argparser.parse_args([]))

        http = credentials.authorize(httplib2.Http())
        self._service = build(self.api_name, self.api_version, http=http)
