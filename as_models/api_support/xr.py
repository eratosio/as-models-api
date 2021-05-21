
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

from .retries import ANY, retry

import numpy as np
import pydap.client
from xarray import Variable
from xarray.core import indexing
from xarray.core.pycompat import integer_types
from xarray.core.utils import Frozen, is_dict_like
from xarray.backends.common import AbstractDataStore, BackendArray

# LazilyOuterIndexedArray and FrozenOrderedDict renamed in later versions of xarray.
try:
    from xarray.core.indexing import LazilyIndexedArray
except ImportError:
    from xarray.core.indexing import LazilyOuterIndexedArray as LazilyIndexedArray

try:
    from xarray.core.utils import FrozenDict
except ImportError:
    from xarray.core.utils import FrozenOrderedDict as FrozenDict


class PydapArrayWrapper(BackendArray):
    def __init__(self, array):
        self.array = array

    @property
    @retry(retryable_methods=ANY)
    def shape(self):
        return self.array.shape

    @property
    @retry(retryable_methods=ANY)
    def dtype(self):
        return self.array.dtype

    def __getitem__(self, key):
        return indexing.explicit_indexing_adapter(key, self.shape, indexing.IndexingSupport.BASIC, self._getitem)

    @retry(retryable_methods=ANY)
    def _getitem(self, key):
        result = self.array[key]

        axis = tuple(n for n, k in enumerate(key) if isinstance(k, integer_types))
        if len(axis) > 0:
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
    @retry(retryable_methods=ANY)
    def open(cls, url, session=None):
        return cls(pydap.client.open_url(url, session=session))

    @retry(retryable_methods=ANY)
    def open_store_variable(self, var):
        data = LazilyIndexedArray(PydapArrayWrapper(var))
        return Variable(var.dimensions, data, _fix_attributes(var.attributes))

    @retry(retryable_methods=ANY)
    def get_variables(self):
        return FrozenDict((k, self.open_store_variable(self.ds[k])) for k in self.ds.keys())

    @retry(retryable_methods=ANY)
    def get_attrs(self):
        return Frozen(_fix_attributes(self.ds.attributes))

    @retry(retryable_methods=ANY)
    def get_dimensions(self):
        return Frozen(self.ds.dimensions)
