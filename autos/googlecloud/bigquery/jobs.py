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
