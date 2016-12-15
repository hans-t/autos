import json


def format_error(error):
    return json.dumps(error, indent=2, ensure_ascii=False, sort_keys=True)


class JobError(Exception):
    def __init__(self, job):
        self.job = job
        self.errors = [{'job_name': job.name}] + job.errors

        job.error_result['job_name'] = job.name
        message = 'Error Result: {}'.format(format_error(job.error_result))
        super().__init__(message)


class CopyError(Exception):
    pass


class MissingSchema(Exception):
    pass


class LoadConfigurationError(Exception):
    pass


class DatasetNotFound(Exception):
    pass


class TableNotFound(Exception):
    pass


class InvalidTableReference(Exception):
    pass
