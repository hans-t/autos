import time
import uuid
import logging
import functools

from autos.utils.csv import write_csv
from .service import Service
from .errors import SheetNotFound
from .errors import ExecutionError
from .errors import SheetAlreadyExists
from .errors import MissingSpreadsheetId


logger = logging.getLogger(__name__)


def generate_sheet_id():
    """Generate random sheet ID."""

    return int(time.time())


class Sheets(Service):
    """Sheets API wrapper to perform common tasks.
    Current API version: v4.

    API Documentations:
    - https://developers.google.com/sheets/reference/rest/v4/spreadsheets
    - https://developers.google.com/sheets/guides/migration
    """

    def __init__(
        self,
        scope='https://www.googleapis.com/auth/drive',
    ):
        super().__init__(
            scope=scope,
            api_name='sheets',
            api_version='v4',
        )
        self._spreadsheet_id = None
        self._metadata = {}
        self._properties = {}

    @property
    def spreadsheets(self):
        return self.service.spreadsheets()

    @property
    def spreadsheet_id(self):
        if self._spreadsheet_id is not None:
            return self._spreadsheet_id
        else:
            raise MissingSpreadsheetId('Please set spreadsheet_id.')

    @spreadsheet_id.setter
    def spreadsheet_id(self, value):
        self._spreadsheet_id = value
        self.reload()

    def reload(self):
        """Refreshes sheets' metadata and properties."""

        self.reload_metadata()
        self.reload_properties()

    @property
    def metadata(self):
        return self._metadata

    def reload_metadata(self):
        """Refreshes sheets metadata."""

        self._metadata = self.spreadsheets.get(
            spreadsheetId=self.spreadsheet_id,
            includeGridData=False,
        ).execute()

    @property
    def properties(self):
        return self._properties

    def reload_properties(self):
        """Refreshes sheets' properties."""

        sheets = self.metadata.get('sheets', [])
        self._properties = {sheet['properties']['title']: sheet['properties'] for sheet in sheets}

    def get_sheet_id(self, sheet_name):
        """Maps sheet name to its id."""

        try:
            return self.properties[sheet_name]['sheetId']
        except KeyError as e:
            raise SheetNotFound('{} does not exist.'.format(sheet_name)) from e

    def execute(self, request, batch):
        """Executes a request if batch is False, else return the request.

        :type request: dict
        :param request: Dict request to be passed to Sheets API.

        :type batch: bool
        :param batch: If true, returns request for batching, else execute immediately.
        """

        if batch:
            return request
        return self.batch_update(request)

    def add(self, name='New Sheet', index=0, row_count=10000, column_count=10, batch=False):
        """Adds a new sheet of size row_count and column_count with the given
        name and positioned at index.

        :type name: str
        :param name: Sheet name.

        :type index: int
        :param index: Sheet position.

        :type row_count: int
        :param row_count: Number of rows in the new sheet.

        :type column_count: int
        :param column_count: Number of columns in the new sheet.

        :type batch: bool
        :param batch: If true, returns request for batching, else execute immediately.
        """

        if name in self.properties:
            raise SheetAlreadyExists('A sheet with the name {} already exists.'.format(name))

        request = {
            'addSheet': {
                'properties': {
                    'sheetId': generate_sheet_id(),
                    'title': name,
                    'index': index,
                    'sheetType': 'GRID',
                    'gridProperties': {
                        'rowCount': row_count,
                        'columnCount': column_count,
                    },
                },
            },
        }
        return self.execute(request, batch)

    def delete(self, sheet_id, batch=False):
        """Deletes sheet by its sheet_id.

        :type sheet_id: int
        :param sheet_id: Sheet ID.

        :type batch: bool
        :param batch: If true, returns request for batching, else execute immediately.
        """

        request = {
            'deleteSheet': {
                'sheetId': sheet_id,
            },
        }
        return self.execute(request, batch)

    def delete_by_name(self, sheet_name, batch=False):
        """Deletes sheet by its name."""

        sheet_id = self.get_sheet_id(sheet_name)
        return self.delete(sheet_id, batch=batch)

    def rename(self, current_sheet_name, new_sheet_name, batch=False):
        """Renames a sheet name.

        :type batch: bool
        :param batch: If true, returns request for batching, else execute immediately.
        """

        request = {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': self.get_sheet_id(current_sheet_name),
                    'title': new_sheet_name,
                },
                'fields': 'title',
            }
        }
        return self.execute(request, batch)

    def reset(self, row_count=10000, column_count=10):
        """Removes all sheets and add a new blank sheet with the given
        numbers of rows and columns.
        """

        sheet_temp_name = uuid.uuid4().hex
        self.batch_update(
            self.add(sheet_temp_name, row_count=row_count, column_count=column_count, batch=True),
            *(self.delete_by_name(title, batch=True) for title in self.properties),
        )
        self.rename(sheet_temp_name, 'Sheet1')

    def batch_update(self, *requests):
        body = { 'requests': requests }
        try:
            response = self.spreadsheets.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body,
            ).execute()
        except Exception as e:
            logger.exception('EXECUTION_ERROR')
            raise ExecutionError from e
        else:
            self.reload()
            return response

    def update_values(self, range, values):
        """Updates rows in range with the given values.

        :type range: str
        :param range: The A1 notation of the values to update.

        :type values: list
        :param values: Rows within the range.
        """

        body = { 'range': range, 'values': values }
        return self.spreadsheets.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range,
            valueInputOption='RAW',
            body=body,
        ).execute()

    def get_values(self, range):
        """Retrieves data in range.

        :type range: str
        :param range: The A1 notation of the values to retrieve.

        :rtype: list
        :returns: Rows within the range.
        """

        response = self.spreadsheets.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range,
        ).execute()
        return response.get('values', [])

    def clear_values(self, sheet_name, batch=False):
        """Clear a sheet of all values while preserving formats.

        :type sheet_name: str
        :param sheet_name: Sheet name.

        :type batch: bool
        :param batch: If true, returns request for batching, else execute immediately.
        """

        sheet_id = self.get_sheet_id(sheet_name)
        request = {
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                },
                'fields': 'userEnteredValue',
            }
        }
        return self.execute(request, batch)

    def extract(self, path, range):
        rows = self.get_values(range=range)
        write_csv(path, rows=rows)
