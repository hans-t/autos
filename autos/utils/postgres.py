__all__ = ['value_placeholder']


def value_placeholder(n_values):
    """Create INSERT PostgreSQL Values placeholder.

    :type n_values: int
    :param n_values: Number of placeholders.

    :rtype: str
    :returns: Placeholder string.
    """

    return '({})'.format(','.join(["'{}'"] * n_values))
