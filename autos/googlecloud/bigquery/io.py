import re
import time
import logging

from google.cloud import bigquery

import autos.constants as constants
from autos.utils.hash import hash_str

from . import errors
from .utils import random_string
from ..storage.bucket import Bucket


logger = logging.getLogger(__name__)


DEFAULT_DELIMITER = constants.DEFAULT_DELIMITER
DEFAULT_ENCODING = constants.DEFAULT_ENCODING


def execute(job):
    '''Begin and poll BigQuery job.'''

    job.begin()
    poll(job)


def poll(job):
    '''Poll BigQuery job.'''

    while job.state != 'DONE':
        job.reload()
        time.sleep(1)
    if job.error_result:
        raise errors.JobError(job)


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
        import_bucket,
        export_bucket,
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

        self.project = project
        self.bq_client = bigquery.Client.from_service_account_json(
            json_credentials_path=json_credentials_path,
            project=project,
        )
        self.import_bucket = Bucket(
            name=import_bucket,
            json_credentials_path=json_credentials_path,
            project=project,
        )
        self.export_bucket = Bucket(
            name=export_bucket,
            json_credentials_path=json_credentials_path,
            project=project,
        )

    def parse_table_reference(self, table_reference):
        pattern = r'^(?:([a-z0-9\-]*):)?(\w+)\.(\w+)$'
        match = re.fullmatch(pattern, table_reference)
        try:
            return match.groups(default=self.project)
        except AttributeError:
            raise errors.InvalidTableReference(table_reference)

    def get_dataset(self, name, create=False, raises=False):
        """Get dataset instance.

        :type name: str
        :param name: Dataset name.

        :type create: boolean
        :param create: If true, dataset will be created if it does not exist,
                       else will throw errors.DatasetNotFound error.

        :rtype: gcloud.bigquery.dataset.Dataset
        :returns: Bigquery dataset instance.
        """

        dataset = self.bq_client.dataset(dataset_name=name)
        if dataset.exists():
            dataset.reload()
        elif create:
            dataset.create()
        elif raises:
            raise errors.DatasetNotFound('{}:{}'.format(self.project, name))
        return dataset

    def get_table(self, dataset_name, name, schema=(), create=False, raises=False):
        """Get table instance.

        :type dataset_name: str
        :param dataset_name: Existing dataset name.

        :type name: str
        :param name: Table name.

        :type create: boolean
        :param create: If true, the table will be created if schema is also given.

        :type schema: list
        :param schema: List of dicts following fields representation in
                       https://cloud.google.com/bigquery/docs/reference/v2/tables#resource-representations.
        """

        dataset = self.get_dataset(name=dataset_name, raises=True)
        table = dataset.table(name=name)
        if table.exists():
            table.reload()
        elif create:
            if schema:
                table.schema = make_bq_schema(schema)
                table.create()
            else:
                raise errors.MissingSchema('You need to provide schema when creating table.')
        elif raises:
            raise errors.TableNotFound('{}:{}.{}'.format(self.project, dataset_name, name))
        return table

    def copy_csv_from(
        self,
        dataset_name,
        table_name,
        paths,
        allow_quoted_newlines=False,
        delimiter=DEFAULT_DELIMITER,
        skip_leading_rows=0,
        write_disposition='WRITE_EMPTY',
        create_disposition='CREATE_NEVER',
        max_bad_records=0,
        schema=(),
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

        :type delimiter: str
        :param delimiter: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.fieldDelimiter

        :type skip_leading_rows: int
        :param skip_leading_rows: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.skipLeadingRows

        :type write_disposition: str
        :param write_disposition: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.writeDisposition

        :type create_disposition: str
        :param create_disposition: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.createDisposition

        :type schema: list
        :param schema: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.skipLeadingRows
        """

        if create_disposition == 'CREATE_IF_NEEDED' and not schema:
            raise errors.LoadConfigurationError(
                'Table does not exist and will be created because you set ' \
                'create_disposition=CREATE_IF_NEEDED, ' \
                'but schema is empty. Please provide schema.'
            )

        job_name = random_string()
        destination_table = self.get_table(dataset_name, table_name)
        source_uris = self.import_bucket.upload_files(paths)
        job = self.bq_client.load_table_from_storage(
            job_name,
            destination_table,
            *source_uris,
        )
        job.source_format = 'CSV'
        job.encoding = DEFAULT_ENCODING
        job.allow_quoted_newlines = allow_quoted_newlines
        job.field_delimiter = delimiter
        job.skip_leading_rows = skip_leading_rows
        job.write_disposition = write_disposition
        job.create_disposition = create_disposition
        job.max_bad_records = max_bad_records
        job.schema = make_bq_schema(schema)
        execute(job)

    def copy_json_from(
        self,
        dataset_name,
        table_name,
        paths,
        write_disposition='WRITE_EMPTY',
        create_disposition='CREATE_NEVER',
        max_bad_records=0,
        schema=(),
    ):
        """Copy JSON files to existing BigQuery table.

        See:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        http://gcloud-python.readthedocs.io/en/latest/bigquery-job.html#gcloud.bigquery.job.LoadTableFromStorageJob

        :type dataset_name: str
        :param dataset_name: Dataset name in which the table lives in.

        :type name: str
        :param name: Table name.

        :type paths: list
        :param paths: List of CSV file paths to copy.

        :type write_disposition: str
        :param write_disposition: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.writeDisposition

        :type create_disposition: str
        :param create_disposition: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.createDisposition

        :type max_bad_records: int
        :param max_bad_records: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.maxBadRecords

        :type schema: list
        :param schema: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.skipLeadingRows
        """

        if create_disposition == 'CREATE_IF_NEEDED' and not schema:
            raise errors.LoadConfigurationError(
                'Table does not exist and will be created because you set ' \
                'create_disposition=CREATE_IF_NEEDED, ' \
                'but schema is empty. Please provide schema.'
            )

        job_name = random_string()
        destination_table = self.get_table(dataset_name, table_name)
        source_uris = self.import_bucket.upload_files(paths)
        job = self.bq_client.load_table_from_storage(
            job_name,
            destination_table,
            *source_uris,
        )
        job.source_format = 'NEWLINE_DELIMITED_JSON'
        job.encoding = DEFAULT_ENCODING
        job.write_disposition = write_disposition
        job.create_disposition = create_disposition
        job.max_bad_records = max_bad_records
        job.schema = make_bq_schema(schema)
        execute(job)

    def execute_query(self, query, priority='BATCH', use_legacy_sql=True):
        """Execute BigQuery query.

        :type query: str
        :param query: BigQuery query to execute.

        :type priority: str
        :param priority: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.priority

        :type use_legacy_sql: bool
        :param use_legacy_sql: See: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.useLegacySql

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
        delimiter=DEFAULT_DELIMITER,
        compression='NONE',
    ):
        """Export BigQuery table to Google Cloud Storage bucket.

        :type table: ``gcloud.bigquery.table.Table``
        :param table: BigQuery table.

        :type prefix: str
        :param prefix: Prefix of exported paths in Google Cloud Storage.
        """

        job_name = random_string()
        gcs_uri = 'gs://{}/{}-*'.format(self.export_bucket.name, prefix)
        job = self.bq_client.extract_table_to_storage(job_name, table, gcs_uri)
        job.compression = compression
        job.field_delimiter = delimiter
        job.print_header = True
        job.destination_format = 'CSV'
        execute(job)

    def copy_csv_to(
        self,
        query,
        dir,
        delimiter=DEFAULT_DELIMITER,
        compression='NONE',
        priority='BATCH',
        use_legacy_sql=True,
    ):
        """Copy BigQuery query result to files. This method is designed to imitate
        the behaviour of 'COPY TO' command of PostgreSQL.

        :type query: str
        :param query: BigQuery query to execute.

        :type dir: str
        :param dir: Destination directory.

        :type delimiter: str
        :param delimiter: CSV file delimiter.

        :type compression: str
        :param compression: File compression, either 'GZIP' or 'NONE'.

        :rtype: list
        :returns: A list of query result files.
        """

        table = self.execute_query(query, priority=priority, use_legacy_sql=use_legacy_sql)
        prefix = table.name
        self.export_table_as_csv(
            table,
            prefix=prefix,
            compression=compression,
            delimiter=delimiter,
        )
        yield from self.export_bucket.download_files(dir, prefix=prefix)

    def dump(self, source, dir, **export_kwargs):
        _, dataset_name, table_name = self.parse_table_reference(source)
        table = self.get_table(dataset_name, table_name)
        prefix = hash_str(table.name)
        self.export_table_as_csv(table, prefix=prefix, **export_kwargs)
        yield from self.export_bucket.download_files(dir, prefix=prefix)
