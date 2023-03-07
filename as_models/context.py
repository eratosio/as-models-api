
from .util import urlparse

from abc import ABCMeta, abstractmethod, abstractproperty
from collections.abc import Mapping

ABC = ABCMeta('ABC', (object,), {})  # compatible with Python 2 *and* 3


class Ports(Mapping):
    def __init__(self):
        self.__ports = {}

    def __getitem__(self, key):
        return self.__ports[key]

    def __iter__(self):
        return iter(self.__ports)

    def __len__(self):
        return len(self.__ports)

    def __getattr__(self, attr):
        try:
            return self.__ports[attr]
        except KeyError:
            raise AttributeError('Unknown attribute "{}"'.format(attr))

    def _add(self, port):  # For use by context classes.
        self.__ports[port.name] = port


class BaseContext(ABC):
    def __init__(self):
        self.__ports = Ports()
        self.__thredds_clients = {}

    def __getattr__(self, attr):
        return getattr(self.__ports, attr)

    def _get_thredds_client(self, url):
        from tds_client import Client

        self._cache_thredds_client(self.thredds_client) # Ensure the "main" client is pre-cached.

        return self._cache_thredds_client(Client(url))

    def _cache_thredds_client(self, client):
        if client is not None:
            # NOTE: following implementation assumes only one Thredds instance
            # per host. This may or may not be true, and may need tweaking in
            # the future.
            _, netloc, _, _, _, _ = urlparse.urlparse(client.context_url)
            return self.__thredds_clients.setdefault(netloc, client)

    @property
    def ports(self):
        return self.__ports

    @abstractmethod
    def update(self, *args, **kwargs):
        pass

    @abstractproperty
    def model_id(self):
        pass

    @abstractproperty
    def sensor_client(self):
        pass

    @abstractproperty
    def analysis_client(self):
        pass

    @abstractproperty
    def thredds_client(self):
        pass

    @abstractproperty
    def thredds_upload_client(self):
        pass
