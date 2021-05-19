
from as_models.api_support.xr import PydapDataStore

from functools import partial
import json
import httpretty
import logging
import os.path
import pydap.client
import unittest
import xarray as xr


logging.getLogger().setLevel(logging.DEBUG)


class MockOpenDAPResource(object):
    def __init__(self, url, dataset, retry_responses=None):
        self._url = url
        self._dataset = dataset
        self._dataset_path = os.path.join(os.path.dirname(__file__), 'resources', 'opendap', dataset)
        self._retry_responses = [] if retry_responses is None else retry_responses
        self._retried_requests = 0

        for ext in ['.das', '.dds', '.dods']:
            request_handler = partial(self._handle_request, ext)
            httpretty.register_uri(httpretty.HEAD, url + ext, body=request_handler)
            httpretty.register_uri(httpretty.GET, url + ext, body=request_handler)

    def add_retry_response(self, status, headers, body):
        self._retry_responses.append((status, headers, json.dumps(body)))
        return status, headers, body

    @property
    def url(self):
        return self._url

    @property
    def request_count(self):
        return len(httpretty.latest_requests())

    @property
    def retried_request_count(self):
        return self._retried_requests

    def _handle_request(self, ext, request, uri, response_headers):
        print(request.method, uri, request.path)

        if request.method == httpretty.GET and self._retry_responses:
            self._retried_requests += 1
            return self._retry_responses.pop(0)

        return 200, {}, self._get_content(ext, request)

    def _get_content(self, ext, request):
        if request.method == httpretty.HEAD:
            return ''

        with open(os.path.join(self._dataset_path, self._dataset + ext), 'r') as f:
            return f.read()


class RetriesTests(unittest.TestCase):
    @httpretty.activate
    def test_xarray_over_opendap_with_server_failure(self):
        url = 'http://senaps.io/thredds/dodsC/mock.nc'
        resource = MockOpenDAPResource(url, 'mock.nc')

        # Initially opening the dataset with PyDAP will cause a couple of requests.
        ds = pydap.client.open_url(url)
        request_count = resource.request_count
        data_store = PydapDataStore(ds)
        self.assertGreater(request_count, 0)
        self.assertEqual(0, resource.retried_request_count)

        # Set up the mock OpenDAP resource to return a rate limit response on the next request.
        resource.add_retry_response(500, {'Retry-After': '1'}, {'status': 'rate limited'})

        # Use xarray to get the dataset data. This should cause further requests, including the one that is retried.
        dataset = xr.open_dataset(data_store, decode_cf=False)
        print(dataset.data[:])

        # Double-check that new requests were actually made.
        self.assertGreater(resource.request_count, request_count)
        self.assertEqual(1, resource.retried_request_count)
