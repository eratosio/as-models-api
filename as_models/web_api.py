
from model_state import PENDING, RUNNING, COMPLETE, TERMINATED, FAILED
from ports import STREAM_PORT, MULTISTREAM_PORT, DOCUMENT_PORT
import python_models, r_models

from sensetdp.api import API
from sensetdp.auth import HTTPBasicAuth, HTTPKeyAuth

from as_client import Client

import bottle, logging, multiprocessing, os, time, traceback, urlparse

_LOG_LEVELS = {
	'DEBUG': logging.DEBUG,
	'INFO': logging.INFO,
	'WARNING': logging.WARNING,
	'ERROR': logging.ERROR,
	'CRITICAL': logging.CRITICAL
}

class _Sentinel(object):
	def __eq__(self, other):
		return self is other
_SENTINEL = _Sentinel()

def _determine_runtime_type(args):
	try:
		return args['type']
	except KeyError:
		entrypoint = args['entrypoint']
		if python_models.is_valid_entrypoint(entrypoint):
			return 'python'
		elif r_models.is_valid_entrypoint(entrypoint):
			return 'r'

class _Port(object):
	def __init__(self, job_context, name, type, direction):
		self._job_context = job_context
		self._name = name
		self._type = type
		self._direction = direction
	
	def _get_update(self):
		return { 'type': self._type }
	
	@classmethod
	def from_json(cls, job_context, name, json):
		type_ = json.get('type', None)
		if type_ is None:
			raise ValueError('Required property "type" is missing.') # TODO: more specific exception type?
		
		subclass = { sub._port_type: sub for sub in cls.__subclasses__() }.get(type_, None)
		if subclass is None:
			raise ValueError('Unsupported port type "{}".'.format(type_)) # TODO: more specific exception type?
		
		return subclass(job_context, name=name, **json)
	
	type = property(lambda self: self._type)
	name = property(lambda self: self._name)
	direction = property(lambda self: self._direction)

class _StreamPort(_Port):
	_port_type = STREAM_PORT
	
	def __init__(self, job_context, **kwargs):
		try:
			self._stream_id = kwargs.pop('streamId')
		except KeyError:
			raise ValueError('Missing required property "streamId"') # TODO: more specific exception type?
		
		super(_StreamPort, self).__init__(job_context, **kwargs)
	
	def _get_update(self):
		if self._stream_id in self._job_context._modified_streams:
			return super(_StreamPort, self)._get_update()
	
	stream_id = property(lambda self: self._stream_id)

class _MultistreamPort(_Port):
	_port_type = MULTISTREAM_PORT
	
	def __init__(self, job_context, **kwargs):
		try:
			self._stream_ids = kwargs.pop('streamIds')
		except KeyError:
			raise ValueError('Missing required property "streamIds"') # TODO: more specific exception type?
		
		super(_MultistreamPort, self).__init__(job_context, **kwargs)
	
	def _get_update(self):
		outdated_streams = self._job_context._modified_streams.intersection(self._stream_ids)
		if outdated_streams:
			return dict(super(_MultistreamPort, self)._get_update(), outdatedStreams=outdated_streams)
	
	stream_ids = property(lambda self: self._stream_ids)

class _DocumentPort(_Port):
	_port_type = DOCUMENT_PORT
	
	def __init__(self, job_context, **kwargs):
		self._value = kwargs.pop('document', None) # NOTE: missing document is ok?
		
		super(_DocumentPort, self).__init__(job_context, **kwargs)
	
	def _get_update(self):
		document = self._job_context._modified_documents.get(self._name, None)
		if document is not None:
			return dict(super(_DocumentPort, self)._get_update(), document=document)
	
	@property
	def value(self):
		return self._value
	
	@value.setter
	def value(self, value):
		self._value = value
		self._job_context.update(modified_documents={ self._name: self._value })

class _SCApiProxy(API):
	def __init__(self, job_context, auth, host, api_root):
		self._job_context = job_context
		
		super(_SCApiProxy, self).__init__(auth, host=host, api_root=api_root)
	
	def create_observations(self, results, streamid):
		super(_SCApiProxy, self).create_observations(results, streamid=streamid)
		
		self._job_context.update(modified_streams=[streamid])

class _LogSender(logging.Handler):
	def __init__(self, sender):
		super(_LogSender, self).__init__(logging.NOTSET)
		
		self._sender = sender
	
	def emit(self, record):
		self.format(record)
		self._sender.send({
			'log': [
				{
					'level': record.levelname,
					'logger': record.name,
					'file': record.filename or None,
					'lineNumber': record.lineno or None,
					'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)) + '.{:03}Z'.format(int(record.msecs)%1000),
					'message': record.message
				}
			]
		})

class _JobContext(object):
	def __init__(self, job_request, sender):
		self.model_id = job_request['modelId']
		self.ports = { k:_Port.from_json(self, k, v) for k,v in job_request.get('ports', {}).iteritems()}
		self.sensor_config = job_request.get('sensorCloudConfiguration', None)
		self.analysis_config = job_request.get('analysisServicesConfiguration', None)
		
		self._sender = sender
		
		self._sensor_client = self._analysis_client = None
		
		self._state =  {}
		self._modified_streams = set()
		self._modified_documents = {}
	
	def update(self, message=_SENTINEL, progress=_SENTINEL, modified_streams=[], modified_documents={}):
		update = { k:v for k,v in {
			'state': RUNNING,
			'message': message,
			'progress': progress
		}.iteritems() if v not in (_SENTINEL, self._state.get(k, _SENTINEL)) }
		
		self._modified_streams.update(modified_streams)
		self._modified_documents.update(modified_documents)
		
		if update:
			self._state.update(update)
			self._sender.send(update)
	
	def terminate(self):
		self._sender.send({ 'state': TERMINATED })
		multiprocessing.current_process().terminate()
	
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

class _JobProcess(object):
	def __init__(self, entrypoint, runtime_type, job_request, sender):
		self._entrypoint = entrypoint
		self._runtime_type = runtime_type
		self._job_request = job_request
		self._sender = sender
	
	def __call__(self):
		# Initialise logging.
		log_level = _LOG_LEVELS.get(self._job_request.get('logLevel', 'INFO'), logging.INFO)
		root_logger = logging.getLogger()
		root_logger.setLevel(log_level)
		root_logger.addHandler(_LogSender(self._sender))
		
		api_logger = logging.getLogger('execution_api')
		
		# Create context.
		context = _JobContext(self._job_request, self._sender)
		
		# Run the model!
		try:
			# TODO: see if the runtime can be made more dynamic
			api_logger.debug('Calling implementation method for model %s...', context.model_id)
			if self._runtime_type == 'python':
				return_value = python_models.run_model(self._entrypoint, context)
			elif self._runtime_type == 'r':
				return_value = r_models.run_model(self._entrypoint, context)
			else:
				raise ValueError('Unsupported runtime type "{}".'.format(self._runtime_type))
			api_logger.debug('Implementation method for model %s returned.', context.model_id)
			
			results = { k:v._get_update() for k,v in context.ports.iteritems() }
			results = { k:v for k,v in results.iteritems() if v }
			
			self._sender.send({
				'state': COMPLETE,
				'progress': 1.0,
				'return': return_value,
				'results': results
			})
		except BaseException as e:
			api_logger.critical('Model failed with exception')
			
			self._sender.send({
				'state': FAILED,
				'exception': traceback.format_exc()
			})

class WebAPI(bottle.Bottle):
	def __init__(self, args):
		super(WebAPI, self).__init__()
		
		self._entrypoint = args['entrypoint'] # TODO: gracefully handle missing entrypoint
		self._port = args.get('port', 8080)
		self._runtime_type = _determine_runtime_type(args) # TODO: gracefully handle invalid runtime type
		
		self._process = self._receiver = None
		self._state = { 'state': PENDING }
		
		self.get('/', callback=self._handle_get)
		self.post('/', callback=self._handle_post)
	
	def run(self):
		super(WebAPI, self).run(host='0.0.0.0', port=self._port)
	
	def _handle_get(self):
		return self._update_state()
	
	def _handle_post(self):
		if self._process is not None:
			return bottle.HTTPResponse({ 'error': 'Cannot submit new job - job already running.' }, status=409)
		
		job_request = bottle.request.json
		if 'modelId' not in job_request:
			return bottle.HTTPResponse({'error', 'Required property "modelId" is missing.'}, status=400)
		
		self._receiver, sender = multiprocessing.Pipe(False)
		self._process = multiprocessing.Process(target=_JobProcess(self._entrypoint, self._runtime_type, job_request, sender))
		self._process.start()
		
		return bottle.HTTPResponse(self._update_state(), status=201)
	
	def _update_state(self):
		if (None not in (self._process, self._receiver)) and (self._state.get('state', None) in (None, PENDING, RUNNING)):
			try:
				while self._receiver.poll():
					update = self._receiver.recv()
					self._state.setdefault('log', []).extend(update.pop('log', []))
					self._state.update(update)
			except EOFError as e:
				print e # TODO: handle better
		
		return self._state
