from webob.request import Request
from webob.exc import HTTPError
from requests.exceptions import (MissingSchema, InvalidSchema,
                                 Timeout)

DEFAULT_TIMEOUT = 120  # 120 seconds = 2 minutes


def create_request_from_session_patched(url, session, timeout=DEFAULT_TIMEOUT,
                                        verify=True):
    try:
        req = Request.blank(url)
        req.environ['webob.client.timeout'] = timeout

        if session.auth:
            session.auth(req)

        return req
    except (MissingSchema, InvalidSchema):
        # Missing schema can occur in tests when the url
        # is not pointing to any resource. Simply pass.
        req = Request.blank(url)
        req.environ['webob.client.timeout'] = timeout
        return req
    except Timeout:
        raise HTTPError('Timeout')
