
from collections import Mapping
from abc import ABCMeta, abstractmethod, abstractproperty

class BasePort(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, context, name, type, direction):
        self.__context = context
        self.__name = name
        self.__type = type
        self.__direction = direction
    
    @property
    def type(self):
        return self.__type
    
    @property
    def name(self):
        return self.__name
    
    @property
    def direction(self):
        return self.__direction
    
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
        return self.document if self.was_supplied else default
    
    @abstractproperty
    def document(self):
        pass
    
    @document.setter
    def document(self, document):
        pass

class BaseGridPort(BasePort):
    def __init__(self, context, name, type, direction):
        super(BaseGridPort, self).__init__(context, name, type, direction)
        
        self.__dataset = None
    
    def get(self, default=None):
        return self.dataset if self.was_supplied else default
    
    @abstractproperty
    def catalog_url(self):
        pass
    
    @abstractproperty
    def dataset_path(self):
        pass
    
    @property
    def dataset(self):
        if self.__dataset is None:
            from tds_client import Dataset
            
            client = self._context.thredds_client if self.catalog_url is None else self._context._get_thredds_client(self.catalog_url)
            
            if client is not None:
                self.__dataset = Dataset.from_url(self.dataset_path, client=client)
        
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
            print 'AAA', attr
            return self.__ports[attr]
        except KeyError:
            raise AttributeError()
    
    def _add(self, port): # For use by context classes.
        self.__ports[port.name] = port

class BaseContext(object):
    __metaclass__ = ABCMeta
    
    def __init__(self):
        self.__ports = Ports()
        self.__thredds_clients = {}
    
    def __getattr__(self, attr):
        print 'BBB', attr
        return getattr(self.__ports, attr)
    
    def _get_thredds_client(self, url):
        from tds_client import Client
        
        # Ensure "main" client is pre-cached
        self._cache_thredds_client(self.thredds_client)
        
        return self._cache_thredds_client(Client(url))
    
    def _cache_thredds_client(self, client):
        if client is not None:
            return self.__thredds_clients.setdefault(client.context_url, client)
    
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
    




































'''class _Port(object):
    def __init__(self, context, name, type, direction):
        self._context = context
        self._name = name
        self._type = type
        self._direction = direction.lower()
    
    type = property(lambda self: self._type)
    name = property(lambda self: self._name)
    direction = property(lambda self: self._direction)

class _StreamPort(_Port):
    def __init__(self, context, name, type, direction, stream_id):
        self._stream_id = stream_id
        
        super(_StreamPort, self).__init__(context, name, type, direction)
    
    stream_id = property(lambda self: self._stream_id)

class _MultistreamPort(_Port):
    def __init__(self, context, name, type, direction, stream_ids):
        self._stream_ids = stream_ids
        
        super(_MultistreamPort, self).__init__(context, name, type, direction)
    
    stream_ids = property(lambda self: self._stream_ids)

class _DocumentPort(_Port):
    def __init__(self, context, name, type, direction, value=None):
        self._value = value
        
        super(_DocumentPort, self).__init__(context, name, type, direction)
    
    @property
    def value(self):
        return self._value
    
    @value.setter
    def value(self, value):
        if value != self._value:
            self._value = value
            self._context.update(modified_documents={ self._name: self._value })

class _GridPort(_Port):
    def __init__(self, context, name, type, direction, catalog, dataset):
        self._catalog_url = catalog
        self._dataset_path = dataset
        
        self._dataset = None
        
        super(_GridPort, self).__init__(context, name, type, direction)
    
    @property
    def catalog_url(self):
        return self._catalog_url
    
    @property
    def dataset_path(self):
        return self._dataset_path
    
    @property
    def dataset(self):
        from tds_client import Client as TDSClient, Dataset as TDSDataset
        
        if self._dataset is None:
            client = self._context.thredds_client if self._catalog_url is None else self._context._get_thredds_client(self._catalog_url)
            
            if client is not None:
                self._dataset = TDSDataset.from_url(self._dataset_path, client=client)
        
        return self._dataset

class _SCApiProxy(API):
    def __init__(self, context, auth, host, api_root):
        self._context = context
        
        super(_SCApiProxy, self).__init__(auth, host=host, api_root=api_root)
    
    def create_observations(self, results, streamid):
        super(_SCApiProxy, self).create_observations(results, streamid=streamid)
        
        self._context.update(modified_streams=[streamid])

class Context(object):
    def __init__(self, model_id=None):
        self.model_id = model_id
        self.ports = {}
        
        self._sensor_client = self._analysis_client = self._thredds_client = None
        self._modified_streams = set()
        self._modified_documents = {}
        
        self._thredds_clients = {}
    
    def set_model_id(self, model_id):
        self.model_id = model_id
        
        return self
    
    def configure_port(self, name, type, direction, stream_id=None, stream_ids=None, value=None):
        if type == ports.STREAM_PORT:
            self.ports[name] = _StreamPort(self, name, type, direction, stream_id)
        elif type == ports.MULTISTREAM_PORT:
            self.ports[name] = _MultistreamPort(self, name, type, direction, stream_ids)
        elif type == ports.DOCUMENT_PORT:
            self.ports[name] = _DocumentPort(self, name, type, direction, value)
        else:
            raise ValueError('Unsupported port type "{}"'.format(type))
        
        return self
    
    def configure_sensor_client(self, url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None):
        url, host, api_root, auth = resolve_service_config(url, scheme, host, api_root, port, username, password, api_key)
        
        self._sensor_client = _SCApiProxy(self, auth, host, api_root)
        
        return self
    
    def configure_analysis_client(self, url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None):
        from as_client import Client
        
        url, host, api_root, auth = resolve_service_config(url, scheme, host, api_root, port, username, password, api_key)
        
        self._analysis_client = Client(url, auth)
        
        return self
    
    def configure_thredds_client(self, url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None):
        from tds_client import Client
        
        url, host, api_root, auth = resolve_service_config(url, scheme, host, api_root, port, username, password, api_key)
        
        self._thredds_client = Client(url, auth)
        
        return self
    
    def configure_clients(self, url='', scheme=None, host=None, port=None, username=None, password=None, api_key=None, sensor_path=None, analysis_path=None, thredds_path=None):
        if sensor_path:
            self.configure_sensor_client(url, scheme, host, sensor_path, port, username, password, api_key)
        if analysis_path:
            self.configure_analysis_client(url, scheme, host, analysis_path, port, username, password, api_key)
        if thredds_path:
            self.configure_thredds_client(url, scheme, host, thredds_path, port, username, password, api_key)
    
    def update(self, message=None, progress=None, modified_streams=[], modified_documents={}):
        # TODO: figure out a good way of handling the "message" and "progress" parameters
        
        self._modified_streams.update(modified_streams)
        self._modified_documents.update(modified_documents)
    
    @property
    def modified_streams(self):
        return self._modified_streams
    
    @property
    def modified_documents(self):
        return self._modified_documents
    
    @property
    def sensor_client(self):
        return self._sensor_client
    
    @property
    def analysis_client(self):
        return self._analysis_client
    
    @property
    def thredds_client(self):
        if self._thredds_client is None and self._thredds_config is not None:
            url, host, api_root, auth = resolve_service_config(**self._tds_config)
            
            # Create session and client.
            session = requests.Session()
            session.auth = auth
            self._tds_client = TDSClient(url, session)
            
            # Add to cache of known clients.
            self._thredds_clients[self._tds_client.context_url] = self._tds_client
        
        return self._tds_client
    
    def _get_thredds_client(self, url):
        self.thredds_client # ensure "main" client is cached 
        
        client = TDSClient(url)
        return self._thredds_clients.setdefault(client.context_url, client)'''
