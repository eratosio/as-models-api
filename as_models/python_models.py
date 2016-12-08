
from ports import STREAM_PORT, MULTISTREAM_PORT, DOCUMENT_PORT

from sensetdp.api import API
from sensetdp.auth import HTTPBasicAuth, HTTPKeyAuth

from as_client import Client

import importlib, os, sys, urlparse

def is_valid_entrypoint(entrypoint):
	entrypoint = os.path.abspath(entrypoint)
	
	return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.py')

def run_model(entrypoint, job_request, args, updater):
	model_id = job_request['modelId']
	
	# Load the model's module.
	model_dir, model_file = os.path.split(entrypoint)
	module_name, module_ext = os.path.splitext(model_file)
	sys.path.append(model_dir)
	module = importlib.import_module(module_name)
	
	# Locate a callable matching the model ID.
	implementation = getattr(module, model_id, None)
	if not callable(implementation):
		raise RuntimeError('Unable to locate callable "{}" in model "{}".'.format(model_id, entrypoint)) # TODO: more specific exception type?
	
	# Run the callable.
	context = _Context(job_request, args, updater)
	implementation(context)

class _Port(object):
	def __init__(self, context, name, type, direction):
		self._context = context
		self._name = name
		self._type = type
		self._direction = direction.lower()
	
	@classmethod
	def from_json(cls, context, name, json):
		type_ = json.get('type', None)
		if type_ is None:
			raise ValueError('Required property "type" is missing.') # TODO: more specific exception type?
		
		subclass = { sub._port_type: sub for sub in cls.__subclasses__() }.get(type_, None)
		if subclass is None:
			raise ValueError('Unsupported port type "{}".'.format(type_)) # TODO: more specific exception type?
		
		return subclass(context, name=name, **json)
	
	type = property(lambda self: self._type)
	name = property(lambda self: self._name)
	direction = property(lambda self: self._direction)

class _StreamPort(_Port):
	_port_type = STREAM_PORT
	
	def __init__(self, context, **kwargs):
		try:
			self._stream_id = kwargs.pop('streamId')
		except KeyError:
			raise ValueError('Missing required property "streamId"') # TODO: more specific exception type?
		
		super(_StreamPort, self).__init__(context, **kwargs)
	
	stream_id = property(lambda self: self._stream_id)

class _MultistreamPort(_Port):
	_port_type = MULTISTREAM_PORT
	
	def __init__(self, context, **kwargs):
		try:
			self._stream_ids = kwargs.pop('streamIds')
		except KeyError:
			raise ValueError('Missing required property "streamIds"') # TODO: more specific exception type?
		
		super(_MultistreamPort, self).__init__(context, **kwargs)
	
	stream_ids = property(lambda self: self._stream_ids)

class _DocumentPort(_Port):
	_port_type = DOCUMENT_PORT
	
	def __init__(self, context, **kwargs):
		self._value = kwargs.pop('document', None) # NOTE: missing document is ok?
		
		super(_DocumentPort, self).__init__(context, **kwargs)
	
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

class _Context(object):
	def __init__(self, job_request, args, updater):
		self.model_id = job_request['modelId']
		self.ports = { k:_Port.from_json(self, k, v) for k,v in job_request.get('ports', {}).iteritems()}
		self.sensor_config = job_request.get('sensorCloudConfiguration', None)
		self.analysis_config = job_request.get('analysisServicesConfiguration', None)
		
		self._updater = updater
		self.debug = args.get('debug', False) or job_request.get('debug', False)
		
		self._sensor_client = self._analysis_client = None
	
	def update(self, *args, **kwargs):
		self._updater.update(*args, **kwargs)
	
	@property
	def sensor_client(self):
		if self._sensor_client is None and self._sensor_config is not None:
			url, host, api_root, auth = _resolve_service_config(self._sensor_config)
			
			self._sensor_client = _SCApiProxy(self, auth, host, api_root)
		
		return self._sensor_client
	
	@property
	def analysis_client(self):
		if self._analysis_client is None and self._analysis_config is not None:
			url, host, api_root, auth = _JobContext._resolve_service_config(self._analysis_config)
			
			self._analysis_client = Client(url, auth)
		
		return self._analysis_client
	
	@staticmethod
	def _resolve_service_config(config):
		# Resolve authentication.
		if 'apiKey' in config:
			auth = HTTPKeyAuth(config['apiKey'], 'apikey')
		elif {'username', 'password'}.issubset(config):
			auth = HTTPBasicAuth(config['username'], config['password'])
		else:
			auth = None
		
		# Resolve API base URL and hostname.
		parts = urlparse.urlparse(config.get('url', ''), scheme='http')
		scheme = config.get('scheme', parts[0])
		host = config.get('host', parts[1])
		api_root = config.get('apiRoot', parts[2])
		if 'port' in config:
			host = '{}:{}'.format(host.partition(':')[0], config['port'])
		url = urlparse.urlunparse((scheme, host, api_root) + parts[3:])
		
		return url, host, api_root, auth
