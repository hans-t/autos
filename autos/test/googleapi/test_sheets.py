import logging
import unittest
import unittest.mock as mock


import autos.googleapi.sheets as sheets
import autos.googleapi.errors as errors



logging.getLogger('autos.googleapi.sheets').setLevel(logging.CRITICAL)


class TestDrive(unittest.TestCase):
    def setUp(self):
        self.sheets = sheets.Sheets()

        patcher = mock.patch.object(sheets.Sheets, 'service')
        self.addCleanup(patcher.stop)
        self.sheets.service = patcher.start()

    def test_api_name_is_correct(self):
        self.sheets.api_name = 'sheets'

    def test_api_name_is_v4(self):
        self.sheets.api_version = 'v4'

    def test_spreadsheets_attribute(self):
        self.sheets.spreadsheets
        self.sheets.service.spreadsheets.assert_called_once_with()

    def test_spreadsheets_id_raises_when_unset(self):
        with self.assertRaises(errors.MissingSpreadsheetId):
            self.sheets.spreadsheet_id

    def test_spreadsheets_id_returns_set_value(self):
        self.sheets._spreadsheet_id = expected = 'spreadsheet_id'
        actual = self.sheets.spreadsheet_id
        self.assertEqual(actual, expected)

    def test_setting_spreadsheets_id(self):
        self.sheets.spreadsheet_id = expected = 'spreadsheet_id'
        actual = self.sheets.spreadsheet_id
        self.assertEqual(actual, expected)

    @mock.patch.object(sheets.Sheets, 'reload', autospec=True)
    def test_setting_spreadsheets_id_calls_reload(self, mock_reload):
        self.sheets.spreadsheet_id = 'spreadsheet_id'
        mock_reload.assert_called_once_with(self.sheets)

    @mock.patch.multiple(
        sheets.Sheets,
        reload_metadata=mock.DEFAULT,
        reload_properties=mock.DEFAULT,
        autospec=True,
    )
    def test_reloads(self, **mocks):
        self.sheets.reload()
        mocks['reload_metadata'].assert_called_once_with(self.sheets)
        mocks['reload_properties'].assert_called_once_with(self.sheets)

    def test_metadata(self):
        self.assertEqual(self.sheets.metadata , self.sheets._metadata)

    def test_reload_metadata(self):
        self.sheets.spreadsheet_id = 'spreadsheet_id'
        self.sheets.reload_metadata()
        self.assertEqual(self.sheets._metadata, self.sheets.spreadsheets.get(
            spreadsheetId='spreadsheet_id',
            includeGridData=False,
        ).execute())

    def test_properties(self):
        self.assertEqual(self.sheets.properties, self.sheets._properties)

    def test_reload_properties(self):
        self.sheets._metadata = {
            'sheets': [
                { 'properties': { 'title': 'abc' }},
                { 'properties': { 'title': 'def' }},
                { 'properties': { 'title': 'ghi' }},
            ],
        }
        self.sheets.reload_properties()
        self.assertEqual(self.sheets._properties, {
            'abc': { 'title': 'abc' },
            'def': { 'title': 'def' },
            'ghi': { 'title': 'ghi' },
        })

    def test_get_sheet_id(self):
        self.sheets._properties = {
            'Sheet1': {'sheetId': 1322}
        }
        self.assertEqual(1322, self.sheets.get_sheet_id('Sheet1'))

    def test_get_sheet_id_raises_when_not_found(self):
        with self.assertRaises(errors.SheetNotFound):
            self.sheets.get_sheet_id('not found')

    def test_execute_returns_request_if_batch_is_true(self):
        mock_request = expected = {'deleteSheet': { 'sheetId': 3213 } }
        actual = self.sheets.execute(mock_request, batch=True)
        self.assertEqual(actual, expected)

    @mock.patch.object(sheets.Sheets, 'batch_update', autospec=True)
    def test_execute_calls_batch_update_immediately_if_batch_is_false(self, mock_batch_update):
        mock_request = {'deleteSheet': { 'sheetId': 3213 } }
        self.sheets.execute(mock_request, batch=False)
        mock_batch_update.assert_called_once_with(self.sheets, mock_request)

    @mock.patch.multiple(
        sheets.Sheets,
        get_sheet_id=mock.DEFAULT,
        delete=mock.DEFAULT,
    )
    def test_delete_by_name(self, **mocks):
        mocks['get_sheet_id'].return_value = '3291'
        retval = self.sheets.delete_by_name('sheeeet')
        mocks['get_sheet_id'].assert_called_once_with('sheeeet')
        mocks['delete'].assert_called_once_with('3291', batch=False)

