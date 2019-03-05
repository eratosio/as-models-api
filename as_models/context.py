
from .ports import OUTPUT_PORT
from .util import urlparse

from abc import ABCMeta, abstractmethod, abstractproperty
from collections import Mapping

ABC = ABCMeta('ABC', (object,), {}) # compatible with Python 2 *and* 3

class BasePort(ABC):
    def __init__(self, context, port):
        self.__context = context
        self.__port = port

    @property
    def type(self):
        return self.__port.type

    @property
    def name(self):
        return self.__port.name

    @property
    def direction(self):
        return self.__port.direction

    @property
    def _context(self):
        return self.__context

    @abstractmethod
    def get(self, default):
        pass

    @abstractproperty
    def was_supplied(self):
        pass

class BaseStreamPort(BasePort):
    def get(self, default=None):
        return self.stream_id if self.was_supplied else default

    @abstractproperty
    def stream_id(self):
        pass

class BaseMultistreamPort(BasePort):
    def get(self, default=None):
        return self.stream_ids if self.was_supplied else default

    @abstractproperty
    def stream_ids(self):
        pass

class BaseDocumentPort(BasePort):
    def get(self, default=None):
        return self.value if self.was_supplied else default

    @abstractproperty
    def value(self):
        pass

    @value.setter
    def value(self, value):
        pass

class BaseCollectionPort(BasePort):
    def __init__(self, context, port, ports):
        super(BaseCollectionPort, self).__init__(context, port)

        for idx, p in enumerate(ports): # necessary to map back to response...
            ports[idx].index = idx

        self.__ports = ports



    def get(self, default=None):
        return self.__ports if self.was_supplied else (default or [])

    def __getitem__(self, i):
        return self.get()[i]

    def __len__(self):
        return len(self.get())

    def __iter__(self):
        return iter(self.get())

    def __str__(self):
        return ','.join(map(str, self.get()))

    def __repr__(self):
        return repr(self.get())


class BaseGridPort(BasePort):
    def __init__(self, context, port):
        super(BaseGridPort, self).__init__(context, port)

        self.__dataset = None

    def get(self, default=None):
        return self.dataset if self.was_supplied else default

    def upload_data(self, data, path=None, client=None, *args, **kwargs):
        # The port MUST be an output port.
        if self.direction != OUTPUT_PORT:
            raise ValueError('The "upload data" operation is only valid for output ports.')

        # If client not supplied, use the context's client.
        if client is None:
            client = self._context.thredds_upload_client
            if client is None:
                raise ValueError('No data upload client configured.')

            # If the port's catalog URL is supplied AND the context has a TDS
            # client configured, then the catalog URLs MUST match (since it is
            # assumed the upload client is configured to upload to the same TDS
            # server).
            client_catalog_url = getattr(self._context.thredds_client, 'catalog_url', None)
            if self.catalog_url != client_catalog_url:
                raise ValueError('Data may only be uploaded to the environment\'s own TDS server.')

        path = path or self.dataset_path

        client.upload_data(data, path, *args, **kwargs)

    @abstractproperty
    def catalog_url(self):
        pass

    @abstractproperty
    def dataset_path(self):
        pass

    @property
    def dataset(self):
        if self.__dataset is None:
            from tds_client import Catalog, Dataset

            client = self._context._get_thredds_client(self.catalog_url)

            if client is not None:
                catalog = Catalog(self.catalog_url, client)
                self.__dataset = Dataset(catalog, self.dataset_path)

        return self.__dataset


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

    def _add(self, port): # For use by context classes.
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
