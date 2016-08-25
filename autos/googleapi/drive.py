"""
See:
https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/index.html
https://developers.google.com/drive/v3/reference/
https://developers.google.com/drive/v3/web/examples/
"""


import os
import time
import logging

from apiclient.http import MediaFileUpload
from apiclient.http import MediaIoBaseDownload

from .service import Service
from .errors import UploadError
from .errors import ExportError
from .errors import DownloadError
from .errors import HttpError


logger = logging.getLogger(__name__)


MAX_RETRIES = 15


def incremental_download(fp, request):
    downloader = MediaIoBaseDownload(fp, request)
    next_chunk = downloader.next_chunk
    done = False
    num_retries = 0
    while not done:
        try:
            _, done = next_chunk()
        except HttpError as e:
            if e.resp.status in (500, 502, 503, 504) and num_retries < MAX_RETRIES - 1:
                time.sleep(2 ** num_retries)
                num_retries += 1
                continue
            else:
                logger.exception('DOWNLOAD_ERROR')
                raise DownloadError from e
        else:
            if num_retries:
                num_retries = 0


class Drive(Service):
    def __init__(
        self,
        scope='https://www.googleapis.com/auth/drive',
    ):
        super().__init__(
            scope=scope,
            api_name='drive',
            api_version='v3',
        )

    def upload(self, src, name=None, parents=(), mime_type=None):
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

    def import_csv_as_gsheet(self, src, name=None, parents=()):
        """Uploads a CSV file to Google drive as Google Sheet.

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

    def import_docx_as_gdoc(self, src, name=None, parents=()):
        """Uploads a Microsoft Word (.docx) file to Google drive as Google Document.

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
            mime_type='application/vnd.google-apps.document',
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

    def export_as(self, file_id, dest, mime_type):
        """Exports a Google docs file.

        Can't use apiclient.http.MediaIoBaseDownload when exporting a Google docs
        file because of infinite loop bug. See:
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

    def export_gsheet_as_csv(self, file_id, dest):
        """Exports a Google Sheet (first sheet only) as CSV file.

        :type file_id: string
        :param file_id: Google Sheet file ID.

        :type dest: string
        :param dest: Destination file path.
        """

        mime_type = 'text/csv'
        self.export_as(file_id, dest=dest, mime_type=mime_type)

    def export_gdoc_as_docx(self, file_id, dest):
        """Exports a Google Doc as Microsoft Word (.docx) file.

        :type file_id: string
        :param file_id: Google Sheet file ID.

        :type dest: string
        :param dest: Destination file path.
        """

        mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        self.export_as(file_id, dest=dest, mime_type=mime_type)
