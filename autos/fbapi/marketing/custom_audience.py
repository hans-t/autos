import time
import logging
from datetime import datetime

import requests
from requests.exceptions import ConnectionError
from facebookads.objects import CustomAudience
from facebookads.exceptions import FacebookRequestError

from autos.utils.iterable import chunk_iterable
from .errors import RetryError
from .errors import OperationError
from .version import API_VERSION


logger = logging.getLogger(__name__)


def get_schema(type):
    if type == 'device_id':
        return CustomAudience.Schema.mobile_advertiser_id
    elif type == 'phone_number':
        return CustomAudience.Schema.phone_hash
    elif type == 'email':
        return CustomAudience.Schema.email_hash
    else:
        raise ValueError("Unknown type. Please choose from ('device_id', 'email', 'phone_number')")


def create(ad_account_id, names):
    """
    :type ad_account_id: int
    :param ad_account_id: Facebook ad account id.

    :type names: list
    :param names: A list of custom audience names string.

    :id_map: dictionary
    :param id_map: Dictionary to contain created custom audiences.
                   The keys are the names of the custom audiences.
                   The values are custom audience IDs.
    """

    audience_ids = {}
    for name in names:
        audience = CustomAudience(parent_id='act_{}'.format(ad_account_id))
        audience.update({
            CustomAudience.Field.name: name,
            CustomAudience.Field.subtype: CustomAudience.Subtype.custom,
        })
        resp = audience.remote_create()
        logger.info('Audience {} created.'.format(name))
        audience_ids[name] = resp.get_id()
    return audience_ids


def delete(audience_ids):
    """
    Delete custom audiences by their IDs.

    :type audience_ids: list
    :param audience_ids: A list of audience IDs.
    """

    for audience_id in audience_ids:
        try:
            resp = CustomAudience(audience_id).remote_delete()
        except FacebookRequestError as e:
            logger.exception(e)
        else:
            logger.info('Audience with id {} was deleted.'.format(audience_id))


def read(audiences, fb_access_token, fields=None):
    """
    Get information about custom audiences.

    :type audiences: tuple
    :param audiences: 2-tuple (audience_name, audience_id).

    :type fb_access_token: string
    :param fb_access_token: Facebook access token.

    :type fields: list
    :param fields: A list of facebook custom audience fields as described in:
                   https://developers.facebook.com/docs/marketing-api/reference/custom-audience
    """

    endpoint_url = 'https://graph.facebook.com/{api_version}/{audience_id}'
    headers = {"Authorization": "Bearer " + fb_access_token}
    if fields is None:
        fields = ['id', 'name', 'approximate_count', 'time_updated', 'time_content_updated']
    params = {'fields': ','.join(fields)}

    for audience_name, audience_id in audiences:
        result = requests.get(
            endpoint_url.format(api_version=API_VERSION, audience_id=audience_id),
            params=params,
            headers=headers,
        ).json()

        if 'time_updated' in result:
            result['time_updated'] = datetime.fromtimestamp(result['time_updated']) \
                                             .isoformat(sep=' ')
        if 'time_content_updated' in result:
            result['time_content_updated'] = datetime.fromtimestamp(result['time_content_updated']) \
                                                     .isoformat(sep=' ')
        yield result


def update(custom_audience, schema, chunks, *, operation='add', max_retry=6):
    """
    Updates custom audience.

    :type custom_audience: facebookads.objects.CustomAudience
    :param custom_audience: An instance of CustomAudience.

    :type schema: string
    :param schema: CustomAudience.Schema attribute.

    :type chunks: list
    :param chunks: A list of list of values to be passed on to Facebook.
                   The values depends on the schema.
                   E.g. if schema is Schema.email_hash, then the values are emails.

    :type operation: string
    :param operation: Only supports 'add' or 'remove'. Case insensitive.

    :type max_retry: int
    :param max_retry: Number of retries in case of timeout.
    """

    operation = operation.lower()
    if operation == 'add':
        _update = custom_audience.add_users
    elif operation == 'remove':
        _update = custom_audience.remove_users
    else:
        raise OperationError("Please choose from ('add', 'remove').")

    for idx, chunk in enumerate(chunks):
        for i in range(max_retry):
            try:
                response = _update(schema=schema, users=chunk)
            except ConnectionError as e:
                logger.warning(e)
                time.sleep(2**i)
            else:
                if response.is_success():
                    break
                else:
                    raise response.error()
        else:
            # if the loop never reached break and exit normally
            raise RetryError('Retried {} times and failed'.format(MAX_RETRY))


def add_users(audience_id, iterable, *, type, chunksize=12000):
    """
    Adds users to custom audience.

    :type audience_id: string
    :param audience_id: Custom audience ID.

    :type iterable: list
    :param iterable: A list of values to be added to custom audience.

    :type type: string
    :param type: A string that indicates what values are being passed to the API.
                 Only device_id, e-mail, and phone numbers are supported.

    :type chunksize: int
    :param chunksize: Number of values that are passed to the API at a time.
    """

    custom_audience = CustomAudience(audience_id)
    schema = get_schema(type)
    chunks = chunk_iterable(iterable, chunksize)
    update(custom_audience, schema, chunks, operation='add')


def remove_users(audience_id, iterable, *, type, chunksize=1000):
    """
    Removes users from custom audience.

    :type audience_id: string
    :param audience_id: Custom audience ID.

    :type iterable: list
    :param iterable: A list of values to be added from custom audience.

    :type type: string
    :param type: A string that indicates what values are being passed to the API.
                 Only device_id, e-mail, and phone numbers are supported.

    :type chunksize: int
    :param chunksize: Number of values that are passed to the API at a time.
                      Setting this to more than 3000, might cause unknown error.
    """

    custom_audience = CustomAudience(audience_id)
    schema = get_schema(type)
    chunks = chunk_iterable(iterable, chunksize)
    update(custom_audience, schema, chunks, operation='remove')

