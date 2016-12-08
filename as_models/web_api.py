
from ports import STREAM_PORT, MULTISTREAM_PORT, DOCUMENT_PORT
from model_state import PENDING, RUNNING, COMPLETE, TERMINATED, FAILED
from sentinel import SENTINEL
import log_levels, python_models, r_models

import bottle, datetime, logging, multiprocessing, time, traceback

def _determine_runtime_type(entrypoint, args):
	try:
		return args.pop('type')
	except KeyError:
		if python_models.is_valid_entrypoint(entrypoint):
			return 'python'
		elif r_models.is_valid_entrypoint(entrypoint):
			return 'r'

class _Updater(object):
	def __init__(self, sender):
		self._sender = sender
		
		self._state = {}
		self._modified_streams = set()
		self._modified_documents = {}
	
	def update(self, message=SENTINEL, progress=SENTINEL, modified_streams=[], modified_documents={}):
		update = { k:v for k,v in {
			'state': RUNNING,
			'message': message,
			'progress': progress
		}.iteritems() if v not in (SENTINEL, self._state.get(k, SENTINEL)) }
		
		print 'ABCD', update, modified_streams, modified_documents
		
		self._modified_streams.update(modified_streams)
		self._modified_documents.update(modified_documents)
		
		if update:
			self._state.update(update)
			self._sender.send(update)
	
	def log(self, message, level=None, file=None, line=None, timestamp=None):
		if level is not None and level not in log_levels.LEVELS:
			raise ValueError('Unsupported log level "{}". Supported values: {}'.format(level, ', '.join(log_levels.LEVELS)))
		
		if timestamp is None:
			timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
		
		log_entry = { k:v for k,v in {
			'message': message,
			'level': level,
			'file': file,
			'line': line,
			'timestamp': timestamp
		}.iteritems() if v is not None }
		
		print 'EFGH', log_entry
		
		self._sender.send({ 'log': [ log_entry ] })

class _LogHandler(logging.Handler):
	def __init__(self, updater):
		super(_LogHandler, self).__init__(logging.NOTSET)
		
		self._updater = updater
	
	def emit(self, record):
		self.format(record)
		self._updater.log(
			message=record.message,
			level=log_levels.from_stdlib_levelno(record.levelno),
			file=record.filename or None,
			line=record.lineno,
			timestamp=time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)) + '.{:03}Z'.format(int(record.msecs)%1000)
		)

class _JobProcess(object):
	def __init__(self, entrypoint, runtime_type, args, job_request, sender):
		self._entrypoint = entrypoint
		self._runtime_type = runtime_type
		self._args = args
		self._job_request = job_request
		self._sender = sender
	
	def __call__(self):
		updater = _Updater(self._sender)
		
		# Initialise logging.
		log_level = self._job_request.get('logLevel', self._args.get('log_level', 'INFO'))
		root_logger = logging.getLogger()
		root_logger.setLevel(log_levels.to_stdlib_levelno(log_level))
		root_logger.addHandler(_LogHandler(updater))
		api_logger = logging.getLogger('execution_api')
		
		# Run the model!
		try:
			# TODO: see if the runtime can be made more dynamic
			model_id = self._job_request['modelId']
			api_logger.debug('Calling implementation method for model %s...', model_id)
			if self._runtime_type == 'python':
				python_models.run_model(self._entrypoint, self._job_request, self._args, updater)
			elif self._runtime_type == 'r':
				r_models.run_model(self._entrypoint, self._job_request, self._args, updater)
			else:
				raise ValueError('Unsupported runtime type "{}".'.format(self._runtime_type))
			api_logger.debug('Implementation method for model %s returned.', model_id)
			
			# Update ports (generate "results").
			# TODO: this could probably be neater.
			mod_streams, mod_docs = updater._modified_streams, updater._modified_documents
			results = {}
			for port_name, port in self._job_request['ports'].iteritems(): # TODO: iterate over model ports instead?
				if port['type'] == STREAM_PORT and port.get('streamId', None) in mod_streams:
					results[port_name] = { 'type': port['type'] }
				elif port['type'] == MULTISTREAM_PORT and not mod_streams.isdisjoint(port.get('streamIds', [])):
					results[port_name] = { 'type': port['type'], 'outdatedStreams': list(mod_streams.intersection(port.get('streamIds', []))) }
				elif port['type'] == DOCUMENT_PORT and port_name in mod_docs:
					results[port_name] = { 'type': port['type'], 'document': mod_docs[port_name] }
			
			self._sender.send({
				'state': COMPLETE,
				'progress': 1.0,
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
		
		# TODO: if entrypoint is a manifest.json file, host the model(s)
		# described within. If entrypoint is a directory, look for a
		# manifest.json within the directory, then host the model(s) described
		# within.
		
		self._entrypoint = args.pop('entrypoint') # TODO: gracefully handle missing entrypoint
		self._port = args.pop('port', 8080)
		self._runtime_type = _determine_runtime_type(self._entrypoint, args) # TODO: gracefully handle invalid runtime types
		self._args = args
		
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
		self._process = multiprocessing.Process(target=_JobProcess(self._entrypoint, self._runtime_type, self._args, job_request, sender))
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
