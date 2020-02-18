"""
See:
https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/index.html
https://developers.google.com/drive/v3/reference/
https://developers.google.com/drive/v3/web/examples/
"""


import os
import time
import logging

from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload

from .service import Service
from .errors import HttpError
from .errors import ExportError
from .errors import UploadError
from .errors import DownloadError
from .errors import CreateFolderError


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
        scopes=['https://www.googleapis.com/auth/drive'],
    ):
        super().__init__(
            scopes=scopes,
            api_name='drive',
            api_version='v3',
        )

    def create_folder(self, name, parents=()):
        metadata = {
            'name': name,
            'parents': parents,
            'mimeType': 'application/vnd.google-apps.folder',
        }

        try:
            return self.service.files().create(
                body=metadata,
                fields='id',
            ).execute()
        except HttpError as e:
            logger.exception('CREATE_FOLDER_ERROR')
            raise CreateFolderError from e

    def upload(self, src, name=None, parents=(), mime_type=None, resumable=True, chunksize=4*1024*1024):
        """Uploads a file to Google Drive and convert it to a Google document.

        Google Drive documentations:
        https://developers.google.com/drive/v3/reference/files/create
        https://developers.google.com/drive/v3/reference/files

        :type src: string
        :param src: Source path to be imported.

        :type name: string
        :param name: File name on Google Drive, default to the source file name.

        :type parents: list
        :param parents: List of Google Drive parent folder IDs.

        :type mime_type: string
        :param mime_type: MIME type of the Google document.

        :type resumable: bool
        :param resumable: True if this is a resumable upload. False means upload in a single request.

        :type chunksize: int
        :param chunksize: File will be uploaded in chunks of this many bytes. Only
                          used if resumable=True. Pass in a value of -1 if the file is to be
                          uploaded in a single chunk. Note that Google App Engine has a 5MB limit
                          on request size, so you should never set your chunksize larger than 5MB,
                          or to -1.

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
        media_body = MediaFileUpload(
            filename=src,
            chunksize=chunksize,
            resumable=resumable,
        )

        try:
            return self.service.files().create(
                body=metadata,
                media_body=media_body,
                fields='id',
            ).execute()
        except HttpError as e:
            logger.exception('UPLOAD_ERROR')
            raise UploadError from e

    def upload_new_revision(self, file_id, src, keep_revision=False):
        media_body = MediaFileUpload(filename=src)
        try:
            return self.service.files().update(
                fileId=file_id,
                media_body=media_body,
                keepRevisionForever=keep_revision,
            ).execute()
        except HttpError as e:
            logger.exception('UPLOAD_ERROR')
            raise UploadError from e

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

    def limit_file_sharing(self, file_id):
        """Limit file from share, copy, or download.

        :type file_id: string
        :param file_id: Google Sheet file ID.
        """

        body = {
            'copyRequiresWriterPermission': True,
            'writersCanShare': False,
            'properties': {
                'preventSharing': 'true'
            }
        }
        return self.service.files().update(
            fileId=file_id,
            body=body,
        ).execute()

    def search(self, query=None, page_size=100, exclude_folder=False):
        """Search for files or folders. A wrapper of files.list().

        :type query: string
        :param query: q parameter of files.list()

        :type page_size: string
        :param page_size: pageSize parameter of files.list()

        :type

        References:
        - https://developers.google.com/drive/api/v3/reference/files/list
        - https://developers.google.com/drive/api/v3/search-files
        - https://developers.google.com/drive/api/v3/ref-search-terms

        """
        if exclude_folder:
            query_chunk = "(mimeType != 'application/vnd.google-apps.folder')"
            if query is None or query == '':
                query = query_chunk
            else:
                query = query + ' and ' + query_chunk

        page_token = None
        while True:
            resp = self.service.files().list(
                q=query,
                pageToken=page_token,
                pageSize=page_size,
            ).execute()

            for file in resp.get('files', []):
                yield file

            page_token = resp.get('nextPageToken', None)
            if page_token is None:
                break
