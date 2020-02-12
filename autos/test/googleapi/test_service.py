import unittest
import unittest.mock as mock

import autos.googleapi.errors as errors
import autos.googleapi.service as service


class TestService(unittest.TestCase):
    def setUp(self):
        self.service = service.Service(
            scope='scope',
            api_name='api_name',
            api_version='api_version',
        )

    def test_service_not_initialized_raises(self):
        with self.assertRaises(errors.ServiceNotInitialized):
            self.service.service

    def test_service_is_not_none_return_service_object(self):
        service_obj = mock.Mock()
        self.service._service = service_obj
        self.assertEqual(service_obj, self.service.service)

    @mock.patch.object(service, 'Storage')
    def test_returns_credentials_when_exists(self, mockStorage):
        mock_credentials = mock.MagicMock()
        mock_credentials.invalid = False
        mockStorage.return_value.get.return_value = mock_credentials
        credentials = self.service._get_credentials(credentials_file='', client_secrets_file='')
        self.assertEqual(mock_credentials, credentials)

    @mock.patch.multiple(
        service,
        Storage=mock.DEFAULT,
        flow_from_clientsecrets=mock.DEFAULT,
        tools=mock.DEFAULT,
        autospec=True,
    )
    def test_run_flow_process_when_credentials_is_none(self, **mocks):
        mocks['Storage'].return_value.get.return_value.invalid = True
        mock_credentials = mock.MagicMock()
        mocks['tools'].run_flow.return_value = mock_credentials
        credentials = self.service._get_credentials(
            credentials_file='credentials_file',
            client_secrets_file='client_secrets_file',
        )
        mocks['flow_from_clientsecrets'].assert_called_once_with('client_secrets_file', scope='scope')
        mocks['tools'].run_flow.assert_called_once_with(
            mocks['flow_from_clientsecrets'].return_value,
            mocks['Storage'].return_value,
            mocks['tools'].argparser.parse_args.return_value,
        )
        mocks['tools'].argparser.parse_args.assert_called_once_with([]),
        self.assertEqual(mock_credentials, credentials)

    @mock.patch.object(service.Service, '_get_credentials')
    @mock.patch.object(service, 'build')
    @mock.patch.object(service, 'httplib2')
    def test_init_service(self, mock_httplib2, mock_build, mock__get_credentials):
        mockHttp = mock_httplib2.Http()
        mock_credentials = mock.MagicMock()
        self.service._get_credentials.return_value = mock_credentials
        self.service.init_service(
            credentials_file='credentials_file',
            client_secrets_file='client_secrets_file',
        )
        self.service._get_credentials.assert_called_once_with(
            'credentials_file',
            'client_secrets_file',
        )
        mock_credentials.authorize.assert_called_once_with(mockHttp)
        mock_build.assert_called_once_with(
            'api_name',
            'api_version',
            http=mock_credentials.authorize.return_value
        )



