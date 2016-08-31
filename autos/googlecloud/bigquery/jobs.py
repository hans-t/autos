import time

from .errors import JobError


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
        raise JobError(job)
