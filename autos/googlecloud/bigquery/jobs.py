import re
import json
import logging
import collections

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .utils import random_delay
from .utils import random_string
from .errors import DatasetNotFound
from .errors import InvalidTableReference


logger = logging.getLogger(__name__)


def format_error(error):
    return json.dumps(error, indent=2, ensure_ascii=False, sort_keys=True)


def handle_error(job):
    error_result = job.error_result
    error_result['job_name'] = job.name
    logger.warning('Error Result: {}'.format(format_error(error_result)))

    errors = [{'job_name': job.name}] + job.errors
    logger.debug('Errors: {}'.format(format_error(errors)))


class Jobs:
    def __init__(self, json_credentials_path, project):
        assert json_credentials_path is not None
        assert project is not None

        self.jobs = []
        self.clients = {}
        self.json_credentials_path = json_credentials_path
        self.project = project
        self.client = self.get_client(
            json_credentials_path=json_credentials_path,
            project=project,
        )

    def parse_table_reference(self, table_reference):
        pattern = r'^(?:([a-z0-9\-]*):)?(\w+)\.(\w+)$'
        match = re.fullmatch(pattern, table_reference)
        try:
            return match.groups(default=self.project)
        except AttributeError:
            raise InvalidTableReference(table_reference)

    def get_client(self, *, json_credentials_path=None, project=None):
        if project is None:
            client = self.client
        elif project in self.clients:
            client = self.clients[project]
        else:
            if json_credentials_path is None:
                json_credentials_path = self.json_credentials_path
            self.clients[project] = client = bigquery.Client.from_service_account_json(
                json_credentials_path=json_credentials_path,
                project=project,
            )
        return client

    def get_table(self, table_reference):
        """Get table instance.

        :type table_reference: str
        :param table_reference: BigQuery table reference of the following format:
                                <project_name>:<dataset_name>.<table_name> or
                                <dataset_name>.<table_name>.
        """

        project, dataset_name, table_name = self.parse_table_reference(table_reference)
        client = self.get_client(project=project)
        dataset = client.dataset(dataset_name=dataset_name)
        try:
            dataset.reload()
        except NotFound:
            raise DatasetNotFound(dataset_name)
        table = dataset.table(name=table_name)
        return table

    def _begin(self):
        self.error_results = []
        queue = collections.deque()
        jobs = self.jobs
        while jobs:
            job = jobs.pop()
            job.begin()
            queue.append(job)
            logger.debug('job_name: {}'.format(job.name))
        return queue

    def run(self):
        queue = self._begin()
        while queue:
            delay_mean = 3/len(queue)
            job = queue.popleft()
            job.reload()
            random_delay(mean=delay_mean)
            if job.state != 'DONE':
                queue.append(job)
            else:
                if job.error_result:
                    handle_error(job)


class QueryJobs(Jobs):
    def add(self, id, query, destination, **opts):
        job_name = '{}_{}'.format(id, random_string())
        job = self.get_client().run_async_query(job_name=job_name, query=query)

        if destination is not None:
            job.destination = self.get_table(destination)

        allow_large_results = opts.pop('allow_large_results', None)
        if allow_large_results is not None:
            job.allow_large_results = allow_large_results

        create_disposition = opts.pop('create_disposition', None)
        if create_disposition is not None:
            job.create_disposition = create_disposition

        default_dataset = opts.pop('default_dataset', None)
        if default_dataset is not None:
            job.default_dataset = default_dataset

        flatten_results = opts.pop('flatten_results', None)
        if flatten_results is not None:
            job.flatten_results = flatten_results

        priority = opts.pop('priority', None)
        if priority is not None:
            job.priority = priority

        use_query_cache = opts.pop('use_query_cache', None)
        if use_query_cache is not None:
            job.use_query_cache = use_query_cache

        use_legacy_sql = opts.pop('use_legacy_sql', None)
        if use_legacy_sql is not None:
            job.use_legacy_sql = use_legacy_sql

        write_disposition = opts.pop('write_disposition', None)
        if write_disposition is not None:
            job.write_disposition = write_disposition

        self.jobs.append(job)


class CopyJobs(Jobs):
    def add(
        self,
        id,
        destination,
        sources,
        **opts
    ):
        job_name = '{}_{}'.format(id, random_string())
        destination_table = self.get_table(destination)
        source_tables = (self.get_table(source) for source in sources)
        job = self.get_client().copy_table(job_name, destination_table, *source_tables)

        create_disposition = opts.pop('create_disposition', None)
        if create_disposition is not None:
            job.create_disposition = create_disposition

        write_disposition = opts.pop('write_disposition', None)
        if write_disposition is not None:
            job.write_disposition = write_disposition

        self.jobs.append(job)
