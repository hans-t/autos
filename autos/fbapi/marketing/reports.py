import csv
import time
import os.path
import logging
import collections

from facebookads.objects import AdAccount

from utils.date import date_range


logger = logging.getLogger(__name__)


class ReportsDownloader:
    # TODO: simplify __init__
    def __init__(self, ad_account_ids, fields, params, columns,
                 wanted_action_types, filename_suffix):
        """
        :type ad_account_ids: list
        :param ad_account_ids: List of ad account IDs.

        :type fields: list
        :param fields: List of requested fields to be passed to Facebook API.

        :type params: dictionary
        :param params: Dictionary of parameters to be passed to Facebook API.

        :type columns: list
        :param columns: List of columns to be written to the reports.

        :type wanted_action_types: dictionary
        :param wanted_action_types:

        :type filename_suffix: string
        :param filename_suffix: Report filenames suffix.
        """

        self.ad_account_ids = ad_account_ids
        self.fields = fields
        self.params = params
        self.columns = columns
        self.wanted_action_types = wanted_action_types
        self.filename_suffix = filename_suffix
        self.current_date = ''

    def submit_async_insights_job(self, ad_account_id, report_date):
        """
        Submit a single ad_insights job.
        """
        ad_account = AdAccount(ad_account_id)
        self.params['time_range'] = {'since': report_date, 'until': report_date}
        logger.info("Submitting job for ad account {}".format(ad_account_id))
        return ad_account.get_insights(self.fields, self.params, async=True)

    def submit_async_insights_jobs(self, report_date):
        """
        :type ad_account_ids: iterable
        :param ad_account_ids: An iterable of ad account ID strings.

        :type report_date: string
        :param report_date: Date of a report to be pulled.
        """
        for ad_account_id in self.ad_account_ids:
            yield self.submit_async_insights_job(ad_account_id, report_date)

    def get_insights(self, jobs):
        """
        Retrieved ad insights from completed async jobs and transform them.
        """
        for job in jobs:
            ad_insights = job.get_result()
            for row in self.transform_insights(ad_insights):
                yield row

    def transform_actions(self, row, actions):
        for action in actions:
            action_type = action['action_type']

            # Filter action_type
            wanted_action_keys = self.wanted_action_types.get(action_type, set())

            # If action_type is wrong, the body of the loop won't be executed.
            # If correct, check whether there are keys that are wanted and get their values.
            for action_key in wanted_action_keys & action.keys():
                row[action_key + '_' + action_type] = action[action_key]

    def transform_insights(self, ad_insights):
        """
        Transform pulled insights into desired rows with specified columns.
        """
        for insight in ad_insights:
            row = {col: insight.get(col, '') or '' for col in self.columns}
            self.transform_actions(row, insight.get('actions', []) or [])
            yield row

    def pull_report_on_date(self, date):
        """
        Get a combined report from each ad_account_id for a particular date.
        """
        self.current_date = date
        jobs = self.submit_async_insights_jobs(report_date=date)
        completed_jobs = self.run_until_complete(jobs)
        for row in self.get_insights(completed_jobs):
            yield row

    def run_until_complete(self, jobs):
        jobs_not_completed = collections.deque(jobs)
        while jobs_not_completed:
            time.sleep(3)
            job = jobs_not_completed.popleft()
            job.remote_read()
            logger.info(str(job))

            job_done = job['async_percent_completion'] == 100 and 'time_completed' in job
            job_failed = job['async_status'] == "Job Failed"
            if job_failed:
                job = self.submit_async_insights_job(
                    ad_account_id='act_{account_id}'.format(**job),
                    report_date=self.current_date,
                )
                jobs_not_completed.append(job)
            elif not job_done:
                jobs_not_completed.append(job)
            else:
                yield job

    def to_csv(self, filename, rows):
        logger.info('Writing to {}'.format(filename))
        with open(filename, mode='w', encoding='utf-8', newline='') as fp:
            writer = csv.DictWriter(fp, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(rows)

    def pull(self, output_dir, since, until):
        for report_date in date_range(since, until):
            rows = self.pull_report_on_date(date=report_date)
            filename = os.path.join(output_dir, report_date + self.filename_suffix + '.csv')
            self.to_csv(filename, rows=rows)

