__all__ = ['dround']

from decimal import Decimal


def dround(decimal_number, decimal_places):
    """Round decimal_number up to decimal_places

    :type decimal_number: decimal.Decimal
    :param decimal_number: Decimal number.

    :type decimal_places: integer
    :param decimal_places: Number of decimal places to keep.

    :rtype: decimal.Decimal
    :returns: Rounded decimal number.
    """

    return decimal_number.quantize(Decimal(10) ** -decimal_places)
