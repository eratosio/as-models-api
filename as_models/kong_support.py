
from requests.packages.urllib3.util.retry import Retry

import logging


class KongRetry(Retry):
    # Kong non-standard rate limiting headers and appropriate backoff times. In each case, we back off half of the rate
    # limit period, under the assumption that it must have taken some time to exhaust the quota. The worst that'll
    # happen is that the first retry will get a 429 and the second succeeds.
    X_RATE_LIMIT_HEADERS = [
        ('X-RateLimit-Remaining-Second', 0.5),
        ('X-RateLimit-Remaining-Minute', 30.0),
        ('X-RateLimit-Remaining-Hour', 1800.0),
    ]
    LOGGER = logging.getLogger('KongRetry')

    def __init__(self, *args, **kwargs):
        self.__backoff_time = kwargs.pop('backoff_time', None)
        super().__init__(*args, **kwargs)

    def new(self, *args, **kwargs):
        kwargs['backoff_time'] = self.__backoff_time
        return super().new(*args, **kwargs)

    def increment(self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None):
        if response and (response.status == 429):
            self.__backoff_time = KongRetry.__backoff_from_headers(response.getheaders())

        return super().increment(method, url, response, error, _pool, _stacktrace)

    def get_backoff_time(self):
        return self.__backoff_time or super().get_backoff_time()

    @staticmethod
    def __backoff_from_headers(headers):
        # Kong's support for various rate-limiting headers is a bit patchy. The documentation claims to *never* support
        # Retry-After (although the source code suggests otherwise), and to only support RateLimit-Reset et al from
        # version 2.0

        # If Retry-After is indeed present, fall-back to default behaviour.
        if 'Retry-After' in headers:
            return

        # If RateLimit-Reset is present, use that value for backoff.
        try:
            return float(headers['RateLimit-Reset'])
        except KeyError:
            pass

        # Otherwise, try to find a Kong X-RateLimit-Remaining header that is at zero.
        for header, backoff in KongRetry.X_RATE_LIMIT_HEADERS:
            if headers.get(header, None) == '0':
                KongRetry.LOGGER.debug('{} exhausted, backing off {} seconds'.format(header, backoff))
                return backoff
