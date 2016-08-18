__all__ = ['get_date_range']

import datetime


def date_range(since=datetime.date.today(), until=datetime.date.today()):
    """Get date range from `since` until `until`.

    :type since: datetime.date
    :param since: Earliest date of the range.

    :type until: datetime.date
    :param until: Latest date of the range.

    :rtype: iterable
    :returns: iterable of datetime.date instances for each date within the range.
    """

    while since <= until:
        yield until
        until -= datetime.timedelta(days=1)


def get_n_days_ago_date(n=1):
    """Get the date n days ago from today. Default to yesterday.

    :type n: int
    :param n: Positive value means future date. Negative value means past date.
              0 means today.
    """
    return (datetime.date.today() - datetime.timedelta(days=n))
