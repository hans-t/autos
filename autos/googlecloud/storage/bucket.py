import os.path

from gcloud import storage



class Bucket:
    """Handles importing and exporting data to/from BigQuery via Google Cloud Storage."""
    def __init__(
        self,
        name,
        json_credentials_path,
        project,
    ):
        """
        :type json_credentials_path: str
        :param json_credentials_path: Path to Google service account JSON credentials.

        :type project: str
        :param project: Google developers console project name.

        :type import_bucket: str
        :param import_bucket: Google Cloud Storage bucket to use when importing data to BigQuery.

        :type export_bucket: str
        :param export_bucket: Google Cloud Storage bucket to use when exporting data from BigQuery.
        """

        self.name = name
        self.project = project
        self.client = storage.Client.from_service_account_json(
            json_credentials_path=json_credentials_path,
            project=project,
        )
        self.bucket = self.client.bucket(name)

    def upload_files(self, paths):
        """Import files to bucket.

        :type paths: list
        :param paths: List of path of files to import.

        :rtype: iterator
        :returns: Iterator of list of GCS paths.
        """

        for path in paths:
            blob = self.bucket.blob(blob_name=os.path.split(path)[1])
            with open(path, 'rb') as fp:
                blob.upload_from_file(file_obj=fp, rewind=True, num_retries=10)
            yield 'gs://{}/{}'.format(self.name, blob.name)

    def download_files(self, dir, prefix):
        """Download files which names match prefix to a directory dir.

        :type dir: str
        :param dir: Destination directory.

        :type prefix: str
        :param prefix: Prefix of paths in Google Cloud Storage to be downloaded.

        :rtype: list
        :returns: A list of downloaded files.
        """

        blobs = self.bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            path = os.path.join(dir, blob.name)
            with open(path, 'wb') as fp:
                blob.download_to_file(fp)
            yield path
