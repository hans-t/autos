import copy


class JobError(Exception):
    def __init__(self, job):
        super().__init__('{reason}: {message}'.format(**job.error_result))
        self.errors = copy.deepcopy(job.errors)


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


