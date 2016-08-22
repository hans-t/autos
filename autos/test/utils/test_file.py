import unittest
import unittest.mock as mock

import autos.utils.file as file


class TestRemoveFile(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(file, 'os', autospec=True)
        self.addCleanup(patcher.stop)
        self.mock_os = patcher.start()

    def test_calls_os_remove_when_file_exists(self):
        path = 'foo.jpg'
        file.remove_file(path)
        self.mock_os.remove.assert_called_once_with(path)

    def test_does_not_raise_when_file_does_not_exist(self):
        path = 'nonexistent.jpg'
        self.mock_os.remove.side_effect = FileNotFoundError
        file.remove_file(path)


class TestRemoveFiles(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(file, 'remove_file', autospec=True)
        self.addCleanup(patcher.stop)
        self.mock_remove_file = patcher.start()

    def test_removes_files(self):
        paths = ['foo.jpg', 'bar.gif', 'wipe.sh']
        file.remove_files(*paths)
        expected_calls = list(map(mock.call, paths))
        actual_calls = self.mock_remove_file.mock_calls
        self.assertEqual(actual_calls, expected_calls)
