"""
Documentation: https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/index.html
"""


import os
import logging

from apiclient.http import MediaFileUpload
from apiclient.http import MediaIoBaseDownload

from .service import Service
from .errors import UploadError
from .errors import ExportError
from .errors import DownloadError
from .errors import HttpError


logger = logging.getLogger(__name__)


def incremental_download(fp, request):
    try:
        downloader = MediaIoBaseDownload(fp, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    except HttpError as e:
        logger.exception('DOWNLOAD_ERROR')
        raise DownloadError from e


class Drive(Service):
    def __init__(
        self,
        scope='https://www.googleapis.com/auth/drive',
        api_name='drive',
        api_version='v3'
    ):
        super().__init__(scope=scope, api_name=api_name, api_version=api_version)

    def upload(self, src, mime_type=None, name=None, parents=()):
        """Uploads a file to Google Drive and convert it to a Google document.

        Google Drive documentations:
        https://developers.google.com/drive/v3/reference/files/create
        https://developers.google.com/drive/v3/reference/files

        :type src: string
        :param src: Source path to be imported.

        :type mime_type: string
        :param mime_type: MIME type of the Google document.

        :type name: string
        :param name: File name on Google Drive, default to the source file name.

        :type parents: list
        :param parents: List of Google Drive parent folder IDs.

        :rtype: string
        :returns: Uploaded file ID.
        """

        filename = os.path.split(src)[1]
        name = name or filename
        metadata = {
            'name': name,
            'parents': parents,
            'mimeType': mime_type,
        }
        media_body = MediaFileUpload(filename=src)

        try:
            file = self.service.files().create(
                body=metadata,
                media_body=media_body,
                fields='id',
            ).execute()
        except HttpError as e:
            logger.exception('UPLOAD_ERROR')
            raise UploadError from e
        return file.get('id')

    def import_csv_as_gsheets(self, src, name=None, parents=()):
        """Uploads a CSV file to Google drive and convert it to Google Sheet.

        :type src: string
        :param src: Path to the csv file to be uploaded.

        :type name: string
        :param name: File name on Google Drive, default to the source file name.

        :type parents: list
        :param parents: List of Google Drive parent folder IDs.

        :rtype: string
        :returns: Uploaded file id.
        """

        return self.upload(
            src=src,
            mime_type='application/vnd.google-apps.spreadsheet',
            name=name,
            parents=parents,
        )

    def download(self, file_id, dest):
        """Downloads a file.

        :type file_id: string
        :param file_id: Google Drive file ID.

        :type dest: string
        :param dest: Destination file path.
        """

        request = self.service.files().get_media(fileId=file_id)
        with open(dest, 'wb') as fp:
            incremental_download(fp, request)

    def export_gdoc(self, file_id, dest, mime_type):
        """Exports a Google docs file.

        Can't use apiclient.http.MediaIoBaseDownload when exporting a Google docs
        file because of infinite loop bug:
        https://github.com/google/google-api-python-client/issues/15

        :type file_id: string
        :param file_id: Google Doc file ID.

        :type dest: string
        :param dest: Destination file path.
        """

        try:
            content = self.service.files().export_media(
                fileId=file_id,
                mimeType=mime_type,
            ).execute()
        except HttpError as e:
            logger.exception('EXPORT_ERROR')
            raise ExportError from e

        with open(dest, 'wb') as fp:
            fp.write(content)

    def export_gsheets_as_csv(self, file_id, dest):
        """Exports Google Sheets as CSV file.

        :type file_id: string
        :param file_id: Google Sheet file ID.

        :type dest: string
        :param dest: Destination file path.
        """

        mime_type = 'text/csv'
        self.export_gdoc(file_id, dest=dest, mime_type=mime_type)
