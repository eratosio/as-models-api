import inspect

from as_models.api_support.retries import ANY, GMT, retry, Retry, RFC_7231_TIMESTAMP_FORMAT
from datetime import datetime, timedelta
import json
import httpretty
import requests
from requests.adapters import HTTPAdapter
import unittest
from webob import Request
from webob.exc import HTTPError as WebObHTTPError


def _webob_raise_for_status(response):
    # Based on an implementation from PyDAP, this method is an equivalent of requests' raise_for_status() (which WebOb
    # lacks natively).
    if response.status_code >= 400:
        raise WebObHTTPError(
            detail=response.status + '\n' + response.text,
            headers=response.headers,
            comment=response.body
        )


class MockJsonResource:
    def __init__(self, url, responses_=None):
        self._url = url
        self._responses = [] if responses_ is None else responses_

        httpretty.register_uri(httpretty.GET, url, body=self)

    def add_response(self, status, headers, body):
        if isinstance(body, dict):
            body_ = json.dumps(body)
        else:
            body_ = body
        self._responses.append((status, headers, body_))
        return status, headers, body

    @property
    def url(self):
        return self._url

    @property
    def request_count(self):
        return len(httpretty.latest_requests())

    def __call__(self, request, uri, response_headers):

        response = self._responses[self.request_count - 1]

        if inspect.isclass(response[2]) and issubclass(response[2], BaseException):
            raise response[2]('mock exception')

        return response


class RetriesTests(unittest.TestCase):
    RETRY_STRATEGY = Retry(
        total=9,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=['HEAD', 'GET', 'OPTIONS', 'PUT', 'DELETE'],
        backoff_factor=1
    )
    HTTP_ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)

    @httpretty.activate
    def test_retry_decorator_connection_error(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(None, None, ConnectionResetError)
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        @retry
        def make_request():
            response_ = requests.get(resource.url)
            response_.raise_for_status()
            return response_

        response = make_request()
        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, response.json())
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_retry_decorator_when_rate_limited(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(429, {'Retry-After': '1'}, {'status': 'rate limited'})
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        @retry
        def make_request():
            response_ = requests.get(resource.url)
            response_.raise_for_status()
            return response_

        response = make_request()
        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, response.json())
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_exceeding_retry_limit_with_decorator(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(429, {'Retry-After': '1'}, {'status': 'rate limited'})
        expected_status, _, expected_body = resource.add_response(429, {'Retry-After': '1'}, {'status': 'rate limited'})
        resource.add_response(200, {}, {'status': 'succeeded'})

        @retry(retries=1)
        def make_request():
            response_ = requests.get(resource.url)
            response_.raise_for_status()
            return response_

        with self.assertRaises(requests.HTTPError) as context:
            make_request()

        response = context.exception.response
        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, response.json())
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_retry_adapter_connection_error(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(None, None, ConnectionResetError)
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        session = requests.Session()
        session.mount('http://', RetriesTests.HTTP_ADAPTER)

        response = session.get(resource.url)

        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, response.json())
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_http_adapter_when_rate_limited(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(429, {'Retry-After': '1'}, {'status': 'rate limited'})
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        session = requests.Session()
        session.mount('http://', RetriesTests.HTTP_ADAPTER)

        response = session.get(resource.url)

        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, response.json())
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_retry_decorator_connection_error_with_webob(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(None, None, ConnectionResetError)
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        @retry(retryable_methods=ANY)  # NOTE: ANY required for WebOb, since request method is not retained.
        def make_request():
            response_ = Request.blank(resource.url).get_response()
            response_.decode_content()
            _webob_raise_for_status(response_)
            return response_

        response = make_request()
        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, json.loads(response.text))
        self.assertEqual(2, resource.request_count)


    @httpretty.activate
    def test_decorator_with_webob(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(429, {'Retry-After': '1'}, {'status': 'rate limited'})
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        @retry(retryable_methods=ANY)  # NOTE: ANY required for WebOb, since request method is not retained.
        def make_request():
            response_ = Request.blank(resource.url).get_response()
            response_.decode_content()
            _webob_raise_for_status(response_)
            return response_

        response = make_request()
        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, json.loads(response.text))
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_http_adapter_when_server_error(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(500, {}, {'status': 'server error'})
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        session = requests.Session()
        session.mount('http://', RetriesTests.HTTP_ADAPTER)

        response = session.get(resource.url)

        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, response.json())
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_server_error_with_webob(self):
        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(500, {}, {'status': 'server error'})
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        @retry(retryable_methods=ANY)  # NOTE: ANY required for WebOb, since request method is not retained.
        def make_request():
            response_ = Request.blank(resource.url).get_response()
            response_.decode_content()
            _webob_raise_for_status(response_)
            return response_

        response = make_request()
        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, json.loads(response.text))
        self.assertEqual(2, resource.request_count)

    @httpretty.activate
    def test_parsing_timestamp_based_retry_header(self):
        # Request no retries until three seconds from now.
        now = datetime.now(GMT)
        retry_after = (now + timedelta(seconds=3)).replace(microsecond=0)
        header_value = retry_after.strftime(RFC_7231_TIMESTAMP_FORMAT)

        resource = MockJsonResource('http://senaps.io/api/test')
        resource.add_response(429, {'Retry-After': header_value}, {'status': 'rate limited'})
        expected_status, _, expected_body = resource.add_response(200, {}, {'status': 'succeeded'})

        @retry
        def make_request():
            response_ = requests.get(resource.url)
            response_.raise_for_status()
            return response_

        response = make_request()

        # Current time MUST be after the retry_after timestamp.
        now = datetime.now(GMT)
        self.assertTrue(now >= retry_after)

        self.assertEqual(expected_status, response.status_code)
        self.assertEqual(expected_body, response.json())
        self.assertEqual(2, resource.request_count)
