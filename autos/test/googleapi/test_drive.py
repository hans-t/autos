import logging
import unittest
import unittest.mock as mock


import autos.googleapi.drive as drive
import autos.googleapi.errors as errors



logging.getLogger('autos.googleapi.drive').setLevel(logging.CRITICAL)


class TestIncrementalDownload(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(drive.time, 'sleep')
        self.addCleanup(patcher.stop)
        self.mock_sleep = patcher.start()

        patcher = mock.patch.object(drive, 'MediaIoBaseDownload')
        self.addCleanup(patcher.stop)
        self.mockMediaIoBaseDownload = patcher.start()

        self.mock_next_chunk = self.mockMediaIoBaseDownload.return_value.next_chunk
        self.mock_fp = mock.MagicMock()
        self.mock_request = mock.MagicMock()

    def test_max_retries_greater_than_0(self):
        self.assertTrue(drive.MAX_RETRIES > 0)

    def test_downloads_until_done(self):
        self.mock_next_chunk.side_effect = [
            (0, False),
            (30, False),
            (70, False),
            (100, True),
        ]
        drive.incremental_download(self.mock_fp, self.mock_request)
        self.assertEqual(self.mock_next_chunk.call_count, 4)

    def test_retries_with_exponential_backoff_for_particular_http_statuses(self):
        mock_resp = mock.MagicMock()
        type(mock_resp).status = mock.PropertyMock(side_effect=[
            500,
            502,
            503,
            504,
        ])
        mock_http_error = errors.HttpError(resp=mock_resp, content=b'')
        self.mock_next_chunk.side_effect = [
            (0, False),
            mock_http_error,
            mock_http_error,
            mock_http_error,
            mock_http_error,
            (70, False),
            (100, True),
        ]
        drive.incremental_download(self.mock_fp, self.mock_request)
        self.assertEqual(self.mock_next_chunk.call_count, 7)
        self.assertEqual(self.mock_sleep.mock_calls, [mock.call(2**i) for i in range(4)])

    def test_raises_download_error_when_retrying_more_than_max_retries(self):
        mock_resp = mock.MagicMock()
        type(mock_resp).status = mock.PropertyMock(return_value=500)
        mock_http_error = errors.HttpError(resp=mock_resp, content=b'')
        self.mock_next_chunk.side_effect = mock_http_error
        with self.assertRaises(errors.DownloadError):
            drive.incremental_download(self.mock_fp, self.mock_request)
        self.assertEqual(self.mock_next_chunk.call_count, drive.MAX_RETRIES)

    def test_raises_download_error_when_http_error_has_unanticipated_status(self):
        mock_resp = mock.MagicMock()
        type(mock_resp).status = mock.PropertyMock(return_value=400)
        mock_http_error = errors.HttpError(resp=mock_resp, content=b'')
        self.mock_next_chunk.side_effect = mock_http_error
        with self.assertRaises(errors.DownloadError):
            drive.incremental_download(self.mock_fp, self.mock_request)
        self.mock_next_chunk.assert_called_once_with()

    def test_after_successful_retries_reset_num_retries_value(self):
        mock_resp = mock.MagicMock()
        type(mock_resp).status = mock.PropertyMock(return_value=502)
        mock_http_error = errors.HttpError(resp=mock_resp, content=b'')
        mock_next_chunk_side_effect = [mock_http_error] * (drive.MAX_RETRIES - 1)
        mock_next_chunk_side_effect.extend([
            (92, False),
            (94, False),
            (99, False),
            (100, True),
        ])
        self.mock_next_chunk.side_effect = mock_next_chunk_side_effect
        drive.incremental_download(self.mock_fp, self.mock_request)
        self.assertEqual(self.mock_next_chunk.call_count, drive.MAX_RETRIES + 3)

class TestDrive(unittest.TestCase):
    def setUp(self):
        self.drive = drive.Drive()

        patcher = mock.patch.object(drive.Drive, 'service')
        self.addCleanup(patcher.stop)
        self.drive.service = patcher.start()

        patcher = mock.patch.object(drive, 'MediaFileUpload')
        self.addCleanup(patcher.stop)
        self.mockMediaFileUpload = patcher.start()

        patcher = mock.patch('builtins.open')
        self.addCleanup(patcher.stop)
        self.mock_open = patcher.start()
        self.mock_fp = self.mock_open.return_value.__enter__.return_value

    def test_api_name_is_correct(self):
        self.drive.api_name = 'drive'

    def test_api_name_is_v3(self):
        self.drive.api_version = 'v3'

    def test_upload_uses_filename_when_name_is_none(self):
        self.drive.upload(src='/tmp/src_path', parents=['folder_id'])
        self.drive.service.files().create.assert_called_once_with(
            body={
                'name': 'src_path',
                'parents': ['folder_id'],
                'mimeType': None,
            },
            media_body=self.mockMediaFileUpload.return_value,
            fields='id',
        )

    def test_upload_uses_name_when_given(self):
        self.drive.upload(src='/tmp/src_path', name='name_i_given', parents=['folder_id'])
        self.drive.service.files().create.assert_called_once_with(
            body={
                'name': 'name_i_given',
                'parents': ['folder_id'],
                'mimeType': None,
            },
            media_body=self.mockMediaFileUpload.return_value,
            fields='id',
        )

    def test_upload_has_mime_type_arg(self):
        self.drive.upload(src='/tmp/src_path', parents=['folder_id'], mime_type='text/csv')
        self.drive.service.files().create.assert_called_once_with(
            body={
                'name': 'src_path',
                'parents': ['folder_id'],
                'mimeType': 'text/csv',
            },
            media_body=self.mockMediaFileUpload.return_value,
            fields='id',
        )

    def test_upload_on_http_error_raises_upload_error(self):
        self.drive \
            .service \
            .files() \
            .create \
            .return_value \
            .execute \
            .side_effect = errors.HttpError(resp=mock.MagicMock(), content=b'')
        with self.assertRaises(errors.UploadError):
            self.drive.upload(src='/tmp/src_path', parents=['folder_id'])

    def test_upload_returns_file_id(self):
        self.drive \
            .service \
            .files() \
            .create \
            .return_value \
            .execute \
            .return_value = { 'id': '392010' }
        file_id = self.drive.upload(src='/tmp/src_path', parents=['folder_id'])
        self.assertEqual(file_id, '392010')

    def test_upload_calls_execute_after_create(self):
        file_id = self.drive.upload(src='/tmp/src_path', parents=['folder_id'])
        self.drive.service.files().create.return_value.execute.assert_called_once_with()

    @mock.patch.object(drive.Drive, 'upload')
    def test_import_csv_as_gsheet(self, mock_upload):
        self.drive.import_csv_as_gsheet(
            src='src_file',
            name='file_name',
            parents=['folder_id'],
        )
        mock_upload.assert_called_once_with(
            src='src_file',
            mime_type='application/vnd.google-apps.spreadsheet',
            name='file_name',
            parents=['folder_id'],
        )

    @mock.patch.object(drive.Drive, 'upload')
    def test_import_docx_as_gdoc(self, mock_upload):
        self.drive.import_docx_as_gdoc(
            src='src_file',
            name='file_name',
            parents=['folder_id'],
        )
        mock_upload.assert_called_once_with(
            src='src_file',
            mime_type='application/vnd.google-apps.document',
            name='file_name',
            parents=['folder_id'],
        )

    @mock.patch.object(drive, 'incremental_download')
    def test_download(self, mock_incremental_download):
        mock_request = self.drive.service.files().get_media.return_value
        self.drive.download(file_id='your_file_id', dest='here')
        self.mock_open.assert_called_once_with('here', 'wb')
        self.drive.service.files().get_media.assert_called_once_with(fileId='your_file_id')
        mock_incremental_download.assert_called_once_with(
            self.mock_fp,
            mock_request,
        )

    def test_export_as_passes_correct_argument_to_export_media(self):
        self.drive.export_as(file_id='your_file_id', dest='here', mime_type='mime_type')
        self.drive \
            .service \
            .files() \
            .export_media \
            .assert_called_once_with(
                 fileId='your_file_id',
                 mimeType='mime_type',
             )

    def test_export_as_calls_execute_after_export_media(self):
        self.drive.export_as(file_id='your_file_id', dest='here', mime_type='mime_type')
        self.drive \
            .service \
            .files() \
            .export_media \
            .return_value \
            .execute \
            .assert_called_once_with()

    def test_export_as_on_http_error_raises_upload_error(self):
        self.drive \
            .service \
            .files() \
            .export_media \
            .return_value \
            .execute \
            .side_effect = errors.HttpError(resp=mock.MagicMock(), content=b'')
        with self.assertRaises(errors.ExportError):
            self.drive.export_as(file_id='your_file_id', dest='here', mime_type='mime_type')

    def test_export_as_writes_content_to_file(self):
        self.drive \
            .service \
            .files() \
            .export_media \
            .return_value \
            .execute \
            .return_value = b'this_is_content'
        self.drive.export_as(file_id='your_file_id', dest='here', mime_type='mime_type')
        self.mock_open.assert_called_once_with('here', 'wb')
        self.mock_fp.write.assert_called_once_with(b'this_is_content')

    @mock.patch.object(drive.Drive, 'export_as')
    def test_export_gsheet_as_csv(self, mock_export_as):
        self.drive.export_gsheet_as_csv('file_id', dest='dest_file_path')
        mock_export_as('file_id', dest='dest_file_path', mime_type='text/csv')

    @mock.patch.object(drive.Drive, 'export_as')
    def test_export_gdoc_as_docx(self, mock_export_as):
        self.drive.export_gdoc_as_docx('file_id', dest='dest_file_path')
        mock_export_as(
            'file_id',
            dest='dest_file_path',
            mime_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )


