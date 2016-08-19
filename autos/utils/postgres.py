__all__ = ['value_placeholder']


def value_placeholder(n_values):
    """Create PostgreSQL Values placeholder. Does not escape single quotes.

    :type n_values: int
    :param n_values: Number of placeholders.

    :rtype: str
    :returns: Value placeholder.
    """

    return '({})'.format(','.join(["'{}'"] * n_values))
