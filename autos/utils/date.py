__all__ = ['date_range', 'get_past_date']

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


def get_past_date(days=0, weeks=0):
    """Get past n days and m weeks ago date. Defaults to today's date.

    :type days: int
    :param days: Number of days ago if positive, later if negative.

    :type weeks: int
    :param weeks: Number of weeks ago if positive, later if negative.
    """

    return (datetime.date.today() - datetime.timedelta(days=days, weeks=weeks))
