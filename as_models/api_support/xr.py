
"""
This module contains an xarray DataStore class that emulates the existing PydapDataStore, but which adds support for our
own HTTP request retry logic. As a result, much of the following code is highly similar to the existing PydapDataStore
code in xarray.

NOTES:
    - this module is called `xr` instead of `xarray` in order to prevent it from shadowing the actual xarray module.
    - PyDAP uses WebOb under the hood. Unfortunately WebOb doesn't report the request method in HTTPErrors, so we have
      to configure the retry decorator to retry on ANY request method. This should be safe, since PyDAP should only ever
      be making HEAD and GET requests anyway.
"""
from .pydap_patches import create_request_from_session_patched
from .retries import ANY, retry

import numpy as np
import pydap.client
from xarray import Variable
from xarray.core import indexing
from xarray.core.pycompat import integer_types
from xarray.core.utils import Frozen, is_dict_like
from xarray.backends.common import AbstractDataStore, BackendArray, robust_getitem
from webob.exc import HTTPError

# LazilyOuterIndexedArray and FrozenOrderedDict renamed in later versions of xarray.
try:
    from xarray.core.indexing import LazilyIndexedArray
except ImportError:
    from xarray.core.indexing import LazilyOuterIndexedArray as LazilyIndexedArray

try:
    from xarray.core.utils import FrozenDict
except ImportError:
    from xarray.core.utils import FrozenOrderedDict as FrozenDict


retry_connection_errors = {ConnectionError, StopIteration, TimeoutError, IndexError}

def raise_for_status_patched(response):
    # Raise error if status is above 300:
    if response.status_code >= 300:
        raise HTTPError(
            detail=response.status+'\n'+response.text,
            status_code=response.status_code,
            code=response.status_code,
            headers=response.headers,
            comment=response.body
        )

pydap.handlers.dap.raise_for_status = raise_for_status_patched

# Monkey patch Pydap to stop it from doing HEAD requests.
pydap.net.create_request_from_session = create_request_from_session_patched


class PydapArrayWrapper(BackendArray):
    def __init__(self, array):
        self.array = array

    @property
    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def shape(self):
        return self.array.shape

    @property
    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def dtype(self):
        return self.array.dtype

    def __getitem__(self, key):
        return indexing.explicit_indexing_adapter(key, self.shape, indexing.IndexingSupport.BASIC, self._getitem)

    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def _getitem(self, key):
        # pull the data from the array attribute if possible, to avoid
        # downloading coordinate data twice
        array = getattr(self.array, "array", self.array)
        result = robust_getitem(array, key, catch=ValueError)
        result = np.asarray(result)
        # in some cases, pydap doesn't squeeze axes automatically like numpy
        axis = tuple(n for n, k in enumerate(key) if isinstance(k, integer_types))
        if result.ndim + len(axis) != array.ndim and axis:
            result = np.squeeze(result, axis)

        return result


def _fix_attributes(attributes):
    attributes = dict(attributes)
    for k in list(attributes):
        if k.lower() == 'global' or k.lower().endswith('_global'):
            # move global attributes to the top level, like the netcdf-C DAP client
            attributes.update(attributes.pop(k))
        elif is_dict_like(attributes[k]):
            # Make Hierarchical attributes to a single level with a dot-separated key
            attributes.update({'{}.{}'.format(k, k_child): v_child for k_child, v_child in attributes.pop(k).items()})
    return attributes


class PydapDataStore(AbstractDataStore):
    def __init__(self, ds):
        self.ds = ds

    @staticmethod
    def from_dataset(dataset):
        """
        Create a PydapDataStore from a tds_client.Dataset instance.

        :param dataset: The dataset to convert to a PydapDataStore.
        :return: The equivalent PydapDataStore.
        """
        return PydapDataStore.open(dataset.opendap.url, dataset.client.session)

    @classmethod
    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def open(cls, url, session=None):
        return cls(pydap.client.open_url(url, session=session, timeout=900))

    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def open_store_variable(self, var):
        data = LazilyIndexedArray(PydapArrayWrapper(var))
        return Variable(var.dimensions, data, _fix_attributes(var.attributes))

    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def get_variables(self):
        return FrozenDict((k, self.open_store_variable(self.ds[k])) for k in self.ds.keys())

    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def get_attrs(self):
        return Frozen(_fix_attributes(self.ds.attributes))

    @retry(retryable_methods=ANY, retryable_connection_errors=retry_connection_errors)
    def get_dimensions(self):
        return Frozen(self.ds.dimensions)
