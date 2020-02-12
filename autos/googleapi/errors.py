from apiclient.errors import HttpError


class CreateFolderError(Exception):
    pass


class UploadError(Exception):
    pass


class ExportError(Exception):
    pass


class DownloadError(Exception):
    pass


class ServiceNotInitialized(Exception):
    pass


class MissingSpreadsheetId(Exception):
    pass


class SheetNotFound(Exception):
    pass


class ExecutionError(Exception):
    pass


class SheetAlreadyExists(Exception):
    pass
