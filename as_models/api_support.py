
"""
Support routines for interacting with Senaps APIs.
"""

from datetime import datetime, timedelta, timezone
import functools
import logging
from requests.packages.urllib3.util.retry import Retry as _Retry
import time


GMT = timezone(timedelta(seconds=0), 'GMT')
RFC_7231_TIMESTAMP_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'

# Default retry parameters.
RETRIES = 10
RETRYABLE_METHODS = {'HEAD', 'GET', 'OPTIONS', 'PUT', 'DELETE'}
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

ANY = 'any'

# Kong's non-standard rate limiting headers and appropriate backoff times. In each case, we back off half of the rate
# limit period, under the assumption that it must have taken some time to exhaust the quota. The worst that'll happen is
# that the first retry will get a 429 and the second succeeds.
_X_RATE_LIMIT_BACKOFFS = [
    ('X-RateLimit-Remaining-Second', 0.5),
    ('X-RateLimit-Remaining-Minute', 30.0),
    ('X-RateLimit-Remaining-Hour', 1800.0)
]

_logger = logging.getLogger(__name__)
_metadata_extractors = []
_supported_libraries = []

# Enable support for Requests, if installed (it should be, but CYA).
try:
    # noinspection PyUnresolvedReferences
    from requests import HTTPError as _HTTPError
    _metadata_extractors.append((_HTTPError, lambda e: (e.request.method, e.response.status_code, e.response.headers)))
    _supported_libraries.append('requests')
except ImportError:
    pass

# Enable support for WebOb, if installed.
try:
    # noinspection PyUnresolvedReferences,PyPackageRequirements
    from webob.exc import HTTPError as _HTTPError

    # NOTE: AFAICT, there's no way to get the request method out of a WebOb HTTPError. As such, we're returning None
    # for the method here. Users of @retry will need to specify retryable_methods='ANY' for retries to work.
    _metadata_extractors.append((_HTTPError, lambda e: (None, e.status_code, e.headers)))
    _supported_libraries.append('webob')
except ImportError:
    pass


def _is_retryable_value(value, retryable_values):
    return (retryable_values is ANY) or (value in retryable_values)


def _parse_retry_delay_header(delay):
    # Potential format #1 is just the number of seconds to wait.
    try:
        return float(delay)
    except ValueError:
        pass

    # Potential format #2 is a timestamp (in RFC 7231 § 7.1.1.2 format) to wait until.
    try:
        timestamp = datetime.strptime(delay, RFC_7231_TIMESTAMP_FORMAT)
    except ValueError:
        raise ValueError('Unable to parse delay header value {}'.format(delay))

    # Timestamp is in GMT (according to the standard), but strptime silently discards the timezone information. We need
    # to add it back in.
    timestamp = timestamp.replace(tzinfo=GMT)

    # If using the timestamp-based format, return number of seconds until timeout is elapsed.
    return (timestamp - datetime.now(GMT)).total_seconds()


def backoff_from_headers(headers):
    """
    Given a mapping containing HTTP response headers, attempt to determine an appropriate retry backoff period.

    :param headers: The HTTP response headers.
    :return: The appropriate backoff period, in seconds, or None if no appropriate period could be determined.
    """

    # Kong's support for various rate-limiting headers is a bit patchy. The documentation claims to *never* support
    # Retry-After (although the source code suggests otherwise), and to only support RateLimit-Reset et al from
    # version 2.0

    # Lower-case header keys for case-insensitive matching.
    headers = {k.lower(): headers[k] for k in headers}

    # First try using Retry-After or RateLimit-Reset headers.
    for header in ['retry-after', 'ratelimit-reset']:
        if header in headers:
            return max(0.0, _parse_retry_delay_header(headers[header]))

    # Otherwise, try to find a Kong X-RateLimit-Remaining header that is at zero.
    for header, backoff in _X_RATE_LIMIT_BACKOFFS:
        if headers.get(header.lower(), '').strip() == '0':
            _logger.debug('{} exhausted, backing off {} seconds'.format(header, backoff))
            return backoff

    _logger.warning('No suitable rate-limiting header found.')


class _Retryable(object):
    def __init__(self, fn, retries, retryable_methods, retryable_statuses):
        functools.update_wrapper(self, fn)

        self.fn = fn
        self._retries = retries
        self._retryable_methods = retryable_methods
        self._retryable_statuses = retryable_statuses

    def __call__(self, *args, **kwargs):
        retries = self._retries

        while True:
            try:
                return self.fn(*args, **kwargs)
            except Exception as e:
                # If we're outta retries, give up now.
                if retries <= 0:
                    raise

                # If unable to back off, re-raise the exception.
                retries -= 1
                if not self._back_off(e):
                    raise

    def _back_off(self, exception):
        for cls, extractor in _metadata_extractors:
            if not isinstance(exception, cls):
                continue

            # Check that the request method and status are retryable.
            method, status, headers = extractor(exception)
            if not _is_retryable_value(method, self._retryable_methods) or not _is_retryable_value(status, self._retryable_statuses):
                return False

            # Sleep for the required backoff interval before retrying.
            backoff = backoff_from_headers(headers)
            _logger.info('HTTP request failed, backing off {} seconds then retrying.'.format(backoff))
            _logger.debug('HTTP error was: {}'.format(exception))
            time.sleep(backoff)
            return True

        return False


def retry(retries=RETRIES, retryable_methods=None, retryable_statuses=None):
    """
    When used to decorate a function, causes that function to be automatically retried if an HTTP error occurs. This
    works by catching exceptions thrown by known HTTP request libraries, examining their contents and - if retrying is
    possible - waiting out an appropriate backoff period before automatically re-invoking the function.

    The supported HTTP libraries on your system are: {}

    :param retries: The maximum number of retries to allow. If omitted, defaults to {}.
    :param retryable_methods: The HTTP methods (e.g. GET, POST, etc) for which to allow retries. If omitted, defaults to
    {}. Provide the ANY constant from this module to allow retry for all HTTP methods.
    :param retryable_statuses: The HTTP status codes (e.g. 200, 401, etc) for which to allow retries. If omitted,
    defaults to {}. Provide the ANY constant from this module to allow retry for all HTTP status codes.
    :return: The wrapped retryable function.
    """.format(', '.join(_supported_libraries), RETRIES, RETRYABLE_METHODS, RETRYABLE_STATUSES)

    # If the 'retries' argument is a callable and the other arguments are their defaults, assume we're being called as a
    # parameterless decorator. Directly wrap the passed callable.
    if callable(retries) and (retryable_methods is None) and (retryable_statuses is None):
        return _Retryable(retries, RETRIES, RETRYABLE_METHODS, RETRYABLE_STATUSES)

    if retryable_methods is None:
        retryable_methods = RETRYABLE_METHODS
    if retryable_statuses is None:
        retryable_statuses = RETRYABLE_STATUSES

    def wrapper(fn):
        return _Retryable(fn, retries, retryable_methods, retryable_statuses)

    return wrapper


class Retry(_Retry):
    """
    An implementation of urllib3.Retry that uses response headers to determine an appropriate time to back off before
    retrying a failed request.
    """

    LOGGER = logging.getLogger('KongRetry')

    def __init__(self, *args, **kwargs):
        self.__backoff_time = kwargs.pop('backoff_time', None)
        super(Retry, self).__init__(*args, **kwargs)

    def new(self, **kwargs):
        kwargs['backoff_time'] = self.__backoff_time
        return super(Retry, self).new(**kwargs)

    def increment(self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None):
        if response and (response.status == 429):
            self.__backoff_time = backoff_from_headers(response.getheaders())

        return super(Retry, self).increment(method, url, response, error, _pool, _stacktrace)

    def get_backoff_time(self):
        return self.__backoff_time or super(Retry, self).get_backoff_time()
