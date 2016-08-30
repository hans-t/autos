import unittest
import unittest.mock as mock

import autos.googlecloud.bigquery.utils as utils


class TestRandomString(unittest.TestCase):
    @mock.patch.object(utils, 'uuid')
    def test_uuid4_string(self, mock_uuid):
        actual = utils.random_string()
        mock_uuid.uuid4.assert_called_once_with()
        expected = mock_uuid.uuid4().hex
        self.assertEqual(actual, expected)
