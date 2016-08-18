import time
import uuid
import logging
import functools

from .service import Service
from .errors import SheetNotFound
from .errors import ExecutionError
from .errors import SheetAlreadyExists
from .errors import MissingSpreadsheetId


logger = logging.getLogger(__name__)


def generate_sheet_id():
    """Generate 32-bit integer for sheet_id."""

    return int(time.time())


class Sheets(Service):
    """Abstraction over Sheets API to simplify operations.

    API Documentations:
    - https://developers.google.com/sheets/reference/rest/v4/spreadsheets
    - https://developers.google.com/sheets/guides/migration
    """

    def __init__(
        self,
        scope='https://www.googleapis.com/auth/drive',
        api_name='sheets',
        api_version='v4'
    ):
        super().__init__(scope=scope, api_name=api_name, api_version=api_version)
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
        self.reload_metadata()
        self.reload_properties()

    @property
    def metadata(self):
        return self._metadata

    def reload_metadata(self):
        self._metadata = self.spreadsheets.get(
            spreadsheetId=self.spreadsheet_id,
            includeGridData=False,
        ).execute()

    @property
    def properties(self):
        return self._properties

    def reload_properties(self):
        sheets = self.metadata.get('sheets', [])
        self._properties = {sheet['properties']['title']: sheet['properties'] for sheet in sheets}

    def get_sheet_id(self, sheet_name):
        """Map sheet name to its id."""

        try:
            return self.properties[sheet_name]['sheetId']
        except KeyError:
            raise SheetNotFound('{} does not exist.'.format(sheet_name))

    def add(self, title='New Sheet', index=0, row_count=10000, column_count=10, batch=False):
        """Add new sheet with the given title and positioned at index."""

        if title in self.properties:
            raise SheetAlreadyExists('A sheet with the name {} already exists.'.format(title))

        request = {
            'addSheet': {
                'properties': {
                    'sheetId': generate_sheet_id(),
                    'title': title,
                    'index': index,
                    'sheetType': 'GRID',
                    'gridProperties': {
                        'rowCount': row_count,
                        'columnCount': column_count,
                    },
                },
            },
        }
        if batch:
            return request
        return self.batch_update(request)

    def delete(self, sheet_id, batch=False):
        """Delete sheet by its sheet_id."""

        request = {
            'deleteSheet': {
                'sheetId': sheet_id,
            },
        }
        if batch:
            return request
        return self.batch_update(request)

    def delete_by_name(self, sheet_name, batch=False):
        """Delete sheet by its name."""

        sheet_id = self.get_sheet_id(sheet_name)
        return self.delete(sheet_id, batch=batch)

    def rename(self, current_sheet_name, new_sheet_name, batch=False):
        """Rename a sheet name."""

        request = {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': self.get_sheet_id(current_sheet_name),
                    'title': new_sheet_name,
                },
                'fields': 'title',
            }
        }

        if batch:
            return request
        return self.batch_update(request)

    def reset(self, row_count=10000, column_count=10):
        """Remove all sheets and add a new blank sheet with the given
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
        body = { 'range': range, 'values': values }
        return self.spreadsheets.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range,
            valueInputOption='RAW',
            body=body,
        ).execute()

    def get_values(self, range):
        response = self.spreadsheets.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range,
        ).execute()
        return response.get('values', [])
