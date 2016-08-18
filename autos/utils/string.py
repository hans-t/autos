__all__ = ['titlecase']

import re


def titlecase(string):
    """Turn string of words into titlecased words.

    :type string: str
    :param string: A string of words.
    """

    return re.sub(
        r"[A-Za-z]+('[A-Za-z]+)?",
        lambda mo: mo.group(0)[0].upper() + mo.group(0)[1:].lower(),
        string,
    )
