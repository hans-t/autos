import os
import uuid
import time
import logging
from collections import deque

from gcloud import bigquery
from gcloud import storage


logger = logging.getLogger(__name__)


class CopyError(Exception):
    pass


class DatasetNotFound(Exception):
    pass


class TableNotFound(Exception):
    pass


class Jobs:
    def __init__(self, jobs=None):
        self.jobs = list(jobs or [])

    def __iter__(self):
        return iter(self.jobs)

    @property
    def error_result(self):
        for job in self:
            if job.error_result:
                yield job.error_result

    @property
    def done(self):
        return all(job.state == 'DONE' for job in self)

    def add(self, job):
        """Add bigquery job

        :type job: `gcloud.bigquery.job._AsyncJob`
        :param job: An instance of a subclass of ``gcloud.bigquery.job._AsyncJob``
        """
        self.jobs.append(job)

    def begin(self):
        """Begin jobs"""
        for job in self:
            job.begin()

    def reload(self):
        for job in self:
            job.reload()

    def poll(self):
        """Check job state periodically until done."""
        while not self.done:
            self.reload()
            time.sleep(1)

    def start(self):
        self.begin()
        self.poll()


def execute(job):
    """Begin and poll BigQuery job."""
    job.begin()
    poll(job)


def poll(job):
    """Poll BigQuery job."""
    while job.state != 'DONE':
        job.reload()
        time.sleep(1)
    if job.error_result:
        raise RuntimeError("{reason}: {message}".format(**job.error_result))


def random_string():
    return uuid.uuid4().hex


def make_bq_schema(schema):
    """Convert schema to BigQuery schema.

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
        bq_schema.append(gcloud.bigquery.table.SchemaField(**field))
    return bq_schema


class BigQueryIO:
    """Handles importing and exporting data to/from BigQuery via Google Cloud Storage."""
    def __init__(self,
                 json_credentials_path,
                 project,
                 import_bucket_name='om_bq_import',
                 export_bucket_name='om_bq_export'):
        """
        :type json_credentials_path: string
        :param json_credentials_path: Path to Google service account JSON credentials.

        :type project: string
        :param project: Google developers console project name.

        :type import_bucket_name: string
        :param import_bucket_name: Google Cloud Storage bucket used as intermediate
                                   storage when importing data to BigQuery.

        :type export_bucket_name: string
        :param export_bucket_name: Google Cloud Storage bucket used as intermediate
                                   storage when exporting data from BigQuery.
        """

        logger.info('Initializing BigQueryIO for project {}.'.format(project))
        self.bq_client = bigquery \
            .Client \
            .from_service_account_json(json_credentials_path, project)

        self.storage_client = storage \
            .Client \
            .from_service_account_json(json_credentials_path, project)

        self.import_bucket_name = import_bucket_name
        self.export_bucket_name = export_bucket_name
        self.import_bucket = self.storage_client.bucket(import_bucket_name)
        self.export_bucket = self.storage_client.bucket(export_bucket_name)

    def init_dataset(self, name, create_if_not_exists=False):
        """
        :type name: string
        :param name: Dataset name.

        :type create_if_not_exists: boolean
        :param create_if_not_exists: If true then dataset will be created if it does not exist,
                                     else will throw DatasetNotFound error.

        :rtype: gcloud.bigquery.dataset.Dataset
        :returns: Bigquery dataset instance.
        """

        dataset = self.bq_client.dataset(dataset_name=name)
        if dataset.exists():
            dataset.reload()
            return dataset
        else:
            if create_if_not_exists:
                dataset.create()
                return dataset
            else:
                raise DatasetNotFound(
                    'The following dataset is not found: {dataset_name}.'
                    .format(dataset_name=name)
                )

    def init_table(self, name, dataset_name, schema=(), create_if_not_exists=False):
        """
        :type name: string
        :param name: Table name.

        :type dataset_name: string
        :param dataset_name: Dataset name in which the table lives in.

        :type schema: list
        :param schema: List of dicts which keys and values follow schema.fields[] in
                       https://cloud.google.com/bigquery/docs/reference/v2/tables#resource-representations.

        :type create_if_not_exists: boolean
        :param create_if_not_exists: If True, then create a new table if schema is given.
        """

        dataset = self.init_dataset(name=dataset_name)
        table = dataset.table(name=name)
        if table.exists():
            table.reload()
            return table
        else:
            if create_if_not_exists and schema:
                table.schema = make_bq_schema(schema)
                table.create()
                return table
            else:
                raise TableNotFound(
                    'The following table is not found: {table_name}.'
                    .format(table_name=name)
                )

    def copy_from(self,
                  dataset_name,
                  table_name,
                  *filenames,
                  field_delimiter=',',
                  skip_leading_rows=0,
                  source_format='CSV',
                  write_disposition='WRITE_TRUNCATE'):
        """Copy files to existing BigQuery table.

        :type destination: string
        :param destination: BigQuery destination table. Format: `dataset_name.table_name`.

        :filenames: string
        :param filenames: Files to be imported to destination table.
        """

        source_uris = self.import_files_to_gcs(*filenames)
        destination_table = self.init_table(
            name=table_name,
            dataset_name=dataset_name
        )
        job = self.bq_client.load_table_from_storage(
            random_string(),
            destination_table,
            *source_uris
        )
        job.field_delimiter = field_delimiter
        job.skip_leading_rows = skip_leading_rows
        job.source_format = source_format
        job.write_disposition = write_disposition
        execute(job)

    def copy_to(self, query, filename, compression='NONE', delimiter='\t'):
        """Execute query on BigQuery and download the CSV result.
        Since, BigQuery can only It requires Google Cloud Storage bucket as temporary file storage.

        :type query: string
        :param query: Query to be executed on BigQuery.

        :type filename: string
        :param filename: Filename of the exported result.

        :type compression: string
        :param compression: File compression, either 'GZIP' or 'NONE'.
        """

        table = self.run_query(query)
        self.export_table(
            table,
            prefix=table.name,
            compression=compression,
            delimiter=delimiter,
        )
        self.export_to_file(filename, prefix=table.name)

    def import_files_to_gcs(self, *filenames):
        bucket = self.import_bucket
        for filename in filenames:
            blob = bucket.blob(blob_name=os.path.split(filename)[1])
            with open(filename, 'rb') as fp:
                blob.upload_from_file(file_obj=fp, num_retries=10)
            yield 'gs://{}/{}'.format(bucket.name, blob.name)

    def run_query(self, query):
        """Run query on BigQuery with default configurations.

        :type query: string
        :param query: Query to be executed on BigQuery.

        :rtype: ``gcloud.bigquery.table.Table``
        :returns: Table instance containing query result.
        """
        job_name = random_string()
        job = self.bq_client.run_async_query(job_name, query=query)
        job.priority = 'BATCH'
        execute(job)
        return job.destination

    def export_table(self, table, prefix, compression, delimiter):
        """Export BigQuery table to Google Cloud Storage bucket.

        :type table: ``gcloud.bigquery.table.Table``
        :param table: BigQuery table.

        :type prefix: string
        :param prefix: Prefix of exported filenames in Google Cloud Storage.
        """
        job_name = random_string()
        destination = 'gs://{}/{}-*.csv.gz'.format(self.export_bucket_name, prefix)
        job = self.bq_client.extract_table_to_storage(job_name, table, destination)
        job.compression = compression
        job.field_delimiter = delimiter
        job.print_header = True
        job.destination_format = 'CSV'
        execute(job)

    def export_to_file(self, filename, prefix):
        """Export files with given prefix to a single file.

        :type filename: string
        :param filename: Destination filename.

        :type prefix: string
        :param prefix: Prefix of filenames in Google Cloud Storage to be downloaded.
        """
        bucket = self.export_bucket
        blobs = bucket.list_blobs(prefix=prefix)
        with open(filename, 'wb') as fp:
            for blob in blobs:
                blob.download_to_file(fp)
