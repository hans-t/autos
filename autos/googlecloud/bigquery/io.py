import logging

from gcloud import bigquery
from gcloud import storage

from .jobs import execute
from .utils import random_string
from .errors import DatasetNotFound


logger = logging.getLogger(__name__)


DEFAULT_DELIMITER = '\t'
DEFAULT_ENCODING = 'UTF-8'


def make_bq_schema(schema):
    """Convert dict schema to BigQuery schema.

    :type schema: list
    :param schema: A list of schema dicts.
    """

    bq_schema = []
    for field in schema:
        field = field.copy()
        field['field_type'] = field_type = field.pop('type')
        sub_fields = field.pop('fields', [])
        if field_type == 'RECORD' and sub_fields:
            field['fields'] = make_bq_schema(sub_fields)
        bq_schema.append(bigquery.table.SchemaField(**field))
    return bq_schema



class BigQueryIO:
    """Handles importing and exporting data to/from BigQuery via Google Cloud Storage."""
    def __init__(
        self,
        json_credentials_path,
        project,
        import_bucket_name,
        export_bucket_name,
    ):
        """
        :type json_credentials_path: str
        :param json_credentials_path: Path to Google service account JSON credentials.

        :type project: str
        :param project: Google developers console project name.

        :type import_bucket_name: str
        :param import_bucket_name: Google Cloud Storage bucket to use when importing data to BigQuery.

        :type export_bucket_name: str
        :param export_bucket_name: Google Cloud Storage bucket to use when exporting data from BigQuery.
        """

        self.project = project
        self.bq_client = bigquery \
            .Client \
            .from_service_account_json(json_credentials_path, project)
        self.storage_client = storage \
            .Client \
            .from_service_account_json(json_credentials_path, project)
        self.import_bucket = self.storage_client.bucket(import_bucket_name)
        self.export_bucket = self.storage_client.bucket(export_bucket_name)

    def get_dataset(self, name, create=False, raises=True):
        """Get dataset instance.

        :type name: str
        :param name: Dataset name.

        :type create: boolean
        :param create: If true, dataset will be created if it does not exist,
                       else will throw DatasetNotFound error.

        :rtype: gcloud.bigquery.dataset.Dataset
        :returns: Bigquery dataset instance.
        """

        dataset = self.bq_client.dataset(dataset_name=name)
        if dataset.exists():
            dataset.reload()
        elif create:
            dataset.create()
        elif raises:
            raise DatasetNotFound('{}:{}'.format(self.project, name))
        return dataset

    def get_table(self, dataset_name, name, create=False, schema=(), raises=True):
        """Get table instance.

        :type dataset_name: str
        :param dataset_name: Dataset name in which the table lives in.

        :type name: str
        :param name: Table name.

        :type create: boolean
        :param create: If true, the table will be created if schema is also given.

        :type schema: list
        :param schema: List of dicts following fields representation in
                       https://cloud.google.com/bigquery/docs/reference/v2/tables#resource-representations.
        """

        dataset = self.get_dataset(name=dataset_name)
        table = dataset.table(name=name)
        if table.exists():
            table.reload()
        elif create and schema:
            table.schema = make_bq_schema(schema)
            table.create()
        elif raises:
            raise TableNotFound('{}:{}.{}'.format(self.project, dataset_name, name))
        return table

    def import_files_to_gcs(self, *paths):
        """Import files to import bucket.

        :type paths: list
        :param paths: List of path of files to import.

        :rtype: iterator
        :returns: Iterator of list of GCS paths.
        """

        bucket = self.import_bucket
        bucket_name = bucket.name
        for path in paths:
            blob = bucket.blob(blob_name=os.path.split(path)[1])
            with open(path, 'rb') as fp:
                blob.upload_from_file(file_obj=fp, rewind=True, num_retries=10)
            yield 'gs://{}/{}'.format(bucket_name, blob.name)

    def copy_csv_from(
        self,
        dataset_name,
        table_name,
        *paths,
        allow_quoted_newlines=False,
        field_delimiter=DEFAULT_DELIMITER,
        skip_leading_rows=0,
        write_disposition='WRITE_TRUNCATE',
    ):
        """Copy CSV files to existing BigQuery table. This method is designed to follow
        the behaviour of 'COPY FROM' command of PostgreSQL.

        See:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        http://gcloud-python.readthedocs.io/en/latest/bigquery-job.html#gcloud.bigquery.job.LoadTableFromStorageJob

        :type dataset_name: str
        :param dataset_name: Dataset name in which the table lives in.

        :type name: str
        :param name: Table name.

        :type paths: list
        :param paths: List of CSV file paths to copy.

        :type allow_quoted_newlines: bool
        :param allow_quoted_newlines: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.allowQuotedNewlines

        :type field_delimiter: str
        :param field_delimiter: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.fieldDelimiter

        :type skip_leading_rows: int
        :param skip_leading_rows: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.skipLeadingRows

        :type write_disposition: str
        :param write_disposition: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.writeDisposition
        """

        source_uris = self.import_files_to_gcs(*paths)
        destination_table = self.get_table(dataset_name, table_name)
        job = self.bq_client.load_table_from_storage(
            random_string(),
            destination_table,
            *source_uris,
        )
        job.create_disposition = 'CREATE_NEVER'
        job.source_format = 'CSV'
        job.encoding = DEFAULT_ENCODING
        job.allow_quoted_newlines = allow_quoted_newlines
        job.field_delimiter = field_delimiter
        job.skip_leading_rows = skip_leading_rows
        job.write_disposition = write_disposition
        execute(job)

    def export_to_file(self, path, prefix):
        """Export files in GCS with filename prefix to a single file.

        :type path: str
        :param path: Destination path.

        :type prefix: str
        :param prefix: Prefix of paths in Google Cloud Storage to be downloaded.
        """

        bucket = self.export_bucket
        blobs = bucket.list_blobs(prefix=prefix)
        with open(path, 'wb') as fp:
            for blob in blobs:
                blob.download_to_file(fp)

    def execute_query(self, query, priority='BATCH', use_legacy_sql=True):
        """Execute BigQuery query.

        :type query: str
        :param query: BigQuery query to execute.

        :rtype: ``gcloud.bigquery.table.Table``
        :returns: Table instance containing query result.
        """

        job_name = random_string()
        job = self.bq_client.run_async_query(job_name, query=query)
        job.priority = priority
        job.use_legacy_sql = use_legacy_sql
        execute(job)
        return job.destination

    def export_table_as_csv(
        self,
        table,
        prefix,
        compression=None,
        delimiter=DEFAULT_DELIMITER,
    ):
        """Export BigQuery table to Google Cloud Storage bucket.

        :type table: ``gcloud.bigquery.table.Table``
        :param table: BigQuery table.

        :type prefix: str
        :param prefix: Prefix of exported paths in Google Cloud Storage.
        """
        job_name = random_string()
        destination = 'gs://{}/{}-*.csv.gz'.format(self.export_bucket_name, prefix)
        job = self.bq_client.extract_table_to_storage(job_name, table, destination)
        job.compression = compression
        job.field_delimiter = delimiter
        job.print_header = True
        job.destination_format = 'CSV'
        execute(job)

    def copy_csv_to(self, query, path, compression=None, delimiter='\t'):
        """Copy BigQuery query result to a file. This method is designed to follow
        the behaviour of 'COPY TO' command of PostgreSQL.

        :type query: str
        :param query: BigQuery query to execute.

        :type path: str
        :param path: Destination path.

        :type compression: str
        :param compression: File compression, either 'GZIP' or None.
        """

        table = self.execute_query(query)
        self.export_table_as_csv(
            table,
            prefix=table.name,
            compression=compression,
            delimiter=delimiter,
        )
        self.export_to_file(path, prefix=table.name)
