
import ports
from util import resolve_service_config

from sensetdp.api import API

class _Port(object):
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
        
        self._sensor_client = self._analysis_client = None
        self._modified_streams = set()
        self._modified_documents = {}
    
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
        
        self._analysis_client = Client(url, auth)
        
        return self
    
    def configure_clients(self, url='', scheme=None, host=None, port=None, username=None, password=None, api_key=None, sensor_path=None, analysis_path=None, thredds_path=None):
		if sensor_path:
			self.configure_sensor_client(url, scheme, host, sensor_path, port, username, password, api_key)
		if analysis_path:
			self.configure_sensor_client(url, scheme, host, analysis_path, port, username, password, api_key)
		if thredds_path:
			self.configure_sensor_client(url, scheme, host, thredds_path, port, username, password, api_key)
    
    def update(self, message=None, progress=None, modified_streams=[], modified_documents={}):
        # TODO: figure out a good way of handling the "message" and "progress" parameters
        
        self._modified_streams.update(modified_streams)
        self._modified_documents.update(modified_documents)
    
    @property
    def sensor_client(self):
        return self._sensor_client
    
    @property
    def analysis_client(self):
        return self._analysis_client
    
    @property
    def modified_streams(self):
        return self._modified_streams
    
    @property
    def modified_documents(self):
        return self._modified_documents
