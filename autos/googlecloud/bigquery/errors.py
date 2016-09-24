import copy


class JobError(Exception):
    def __init__(self, job):
        message = str(job.errors)
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


