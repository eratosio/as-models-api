# -*- coding: UTF-8 -*-
import datetime
import json

from .constants import MAX_ERR_DATA_LEN
from senaps_sensor.auth import HTTPBasicAuth, HTTPKeyAuth

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse


def resolve_service_config(url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None, apiRoot=None, apiKey=None, verify=True):
    api_root = api_root or apiRoot
    api_key = api_key or apiKey
    
    # Resolve authentication.
    if api_key is not None:
        auth = HTTPKeyAuth(api_key, 'apikey')
    elif None not in (username, password):
        auth = HTTPBasicAuth(username, password)
    else:
        auth = None
    
    # Resolve API base URL and hostname.
    parts = urlparse.urlparse(url, scheme='http')
    scheme = parts[0] if scheme is None else scheme
    host = parts[1] if host is None else host
    api_root = parts[2] if api_root is None else api_root
    if port is not None:
        host = '{}:{}'.format(host.partition(':')[0], port)
    url = urlparse.urlunparse((scheme, host, api_root) + parts[3:])
    
    return url, host, api_root, auth, verify


def dump_to_json(token):
    """
    HACK: Seek forgiveness rather than permission: check if we can jsonify token.
    WIP: todo: if you have python3 only, you can use functools.singledispatch to avoid
            this hack.
    :param token: object: something we hope can be jsonified.
    :return:
    """
    try:
        val = json.dumps(token)
        return val
    except TypeError:
        if type(token) == datetime.date or type(token) == datetime.datetime:
            # naive timezone treatment; todo: we might like to expose python-dateutil in the baseimage.
            return token.isoformat()

        return 'error, cannot serialise field,' \
               ' invalid datatype "{0}" for json'.format(type(token).__qualname__)
        #
    except Exception:
        # that was unexpected, guess it is not serializable either...
        return 'error, cannot serialise field, unexpected exception.'


def sanitize_dict_for_json(mapping):
    """
    Iterate over items in 'mapping' and ensure they are types supported by JSON.
    If a type is not supported, we replace the value by a string warning that it is not supported.
    Datetimes and dates get special treatment and will be string serialised because its such a common operation.

    NB: we intentionally skip data encoding of keys that are of an invalid type (non-basic '__str__' func).
    We could implement a filter of sorts to check keys for validity, but it doesn't solve
    the problem that if we detect we cannot serialise to string, what should we do with it?

    :param mapping: dict: a dict of str: object mappings.
    :return: dict, potentially changed.
    """
    json_data = json.dumps(mapping, default=dump_to_json, skipkeys=True)
    if len(json_data) > MAX_ERR_DATA_LEN:
        return {'error': 'json_serialisation_failed, user data larger than max of %s characters. Data preview: %s' % \
               (MAX_ERR_DATA_LEN, json_data[0:150])}
    return json.loads(json_data)
