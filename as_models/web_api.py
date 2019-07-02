
from __future__ import print_function

from .ports import STREAM_PORT, MULTISTREAM_PORT, DOCUMENT_PORT
from .model_state import PENDING, RUNNING, COMPLETE, TERMINATED, FAILED
from .sentinel import Sentinel
from .manifest import Manifest
from . import log_levels, python_models, r_models

import copy
import datetime, json, logging, multiprocessing, os, signal, sys, time, traceback
from flask import Flask, jsonify, make_response, request

_SENTINEL = Sentinel()

def _signalterm_handler(signal, stack):
    exit(0)

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

    def update(self, message=_SENTINEL, progress=_SENTINEL, modified_streams=[], modified_documents={}):
        update = { k:v for k,v in {
            'state': RUNNING,
            'message': message,
            'progress': progress
        }.items() if v not in (_SENTINEL, self._state.get(k, _SENTINEL)) }

        self._modified_streams.update(modified_streams)
        self._modified_documents.update(modified_documents)

        if update:
            self._state.update(update)
            self._sender.send(update)

    def log(self, message, level=None, file=None, line=None, timestamp=None, logger=None):
        if level is not None and level not in log_levels.LEVELS:
            raise ValueError('Unsupported log level "{}". Supported values: {}'.format(level, ', '.join(log_levels.LEVELS)))

        if timestamp is None:
            timestamp = datetime.datetime.utcnow().isoformat() + 'Z'

        log_entry = {
            'message': message,
            'level': level,
            'file': file,
            'lineNumber': line,
            'timestamp': timestamp,
            'logger': logger
        }

        self._sender.send({ 'log': [ log_entry ] })

class _JobProcessLogHandler(logging.Handler):
    def __init__(self, updater):
        super(_JobProcessLogHandler, self).__init__(logging.NOTSET)

        self._updater = updater

    def emit(self, record):
        self.format(record)
        self._updater.log(
            message=record.message,
            level=log_levels.from_stdlib_levelno(record.levelno),
            file=record.filename or None,
            line=record.lineno,
            timestamp=time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)) + '.{:03}Z'.format(int(record.msecs)%1000),
            logger=record.name
        )

class StreamRedirect(object):
    def __init__(self, original_stream, updater, log_level):
        self._original_stream = original_stream
        self._updater = updater
        self._log_level = log_level

    def write(self, string):
        self._original_stream.write(string)
        self._updater.log(string, level=self._log_level)

    def flush(self):
        self._original_stream.flush()

class _JobProcess(object):
    def __init__(self, entrypoint, manifest, runtime_type, args, job_request, sender, logger):
        self._entrypoint = entrypoint
        self._manifest = manifest
        self._runtime_type = runtime_type
        self._args = args
        self._job_request = job_request
        self._sender = sender
        self._logger = logger

    def __call__(self):
        signal.signal(signal.SIGTERM, _signalterm_handler)

        updater = _Updater(self._sender)

        # Initialise logging.
        sys.stdout = StreamRedirect(sys.stdout, updater, log_levels.STDOUT)
        sys.stderr = StreamRedirect(sys.stderr, updater, log_levels.STDERR)
        log_level = self._job_request.get('logLevel', self._args.get('log_level', 'INFO'))
        root_logger = logging.getLogger()
        root_logger.addHandler(_JobProcessLogHandler(updater))
        root_logger.setLevel(log_levels.to_stdlib_levelno(log_level))
        logger = logging.getLogger('JobProcess')

        # Run the model!
        try:
            # TODO: see if the runtime can be made more dynamic
            model_id = self._job_request['modelId']
            logger.debug('Calling implementation method for model %s...', model_id)
            if self._runtime_type == 'python':
                python_models.run_model(self._entrypoint, self._manifest, self._job_request, self._args, updater)
            elif self._runtime_type == 'r':
                r_models.run_model(self._entrypoint, self._manifest, self._job_request, self._args, updater)
            else:
                raise ValueError('Unsupported runtime type "{}".'.format(self._runtime_type))
            _model_complete.value = 1
            logger.debug('Implementation method for model %s returned.', model_id)

            # Update ports (generate "results").
            # TODO: this could probably be neater.
            mod_streams, mod_docs = updater._modified_streams, updater._modified_documents
            results = {}
            bindings = self._job_request['ports']
            for port in self._manifest.models[model_id].ports:
                try:
                    binding = bindings[port.name]
                except KeyError:
                    continue

                if port.type == STREAM_PORT and binding.get('streamId') in mod_streams:
                    results[port.name] = { 'type': port.type }
                elif port.type == MULTISTREAM_PORT and not mod_streams.isdisjoint(binding.get('streamIds', [])):
                    results[port.name] = { 'type': port.type, 'outdatedStreams': list(mod_streams.intersection(binding.get('streamIds', []))) }
                elif port.type == DOCUMENT_PORT and port.name in mod_docs:
                    results[port.name] = { 'type': port.type, 'document': mod_docs[port.name] }

            self._sender.send({
                'state': COMPLETE,
                'progress': 1.0,
                'results': results
            })
        except BaseException as e:
            logger.critical('Model failed with exception')

            self._sender.send({
                'state': FAILED,
                'exception': traceback.format_exc()
            })

class _WebAPILogHandler(logging.Handler):
    def __init__(self, state):
        super(_WebAPILogHandler, self).__init__(logging.NOTSET)

        self._state = state

    def emit(self, record):
        self.format(record)

        self._state.setdefault('log', []).append({
            'message': record.message,
            'level': log_levels.from_stdlib_levelno(record.levelno),
            'file': record.filename or None,
            'lineNumber': record.lineno,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)) + '.{:03}Z'.format(int(record.msecs)%1000),
            'logger': record.name
        })

app = Flask(__name__)

_process = _receiver = None
_state = { 'state': PENDING }
_model_complete = multiprocessing.Value('i', 0)

_root_logger = logging.getLogger()
_root_logger.addHandler(_WebAPILogHandler(_state))

_logger = logging.getLogger('WebAPI')

logging.getLogger('werkzeug').setLevel(logging.ERROR) # disable unwanted Flask HTTP request logs

def _load_entrypoint(path):
    path = os.path.abspath(path)

    # Locate manifest, if possible.
    if os.path.isfile(path):
        head, tail = os.path.split(path)
        manifest_path = path if (tail == 'manifest.json') else os.path.join(head, 'manifest.json')
    elif os.path.isdir(path):
        manifest_path = os.path.join(path, 'manifest.json')
    else:
        raise RuntimeError('Unable to load model from path {} - path does not exist.'.format(path))

    # Try to load the manifest.
    try:
        with open(manifest_path, 'r') as f:
            manifest = Manifest(json.load(f))
    except Exception as e:
        raise RuntimeError('Failed to read manifest for model at {}: {}'.format(path, e))

    return manifest, os.path.join(os.path.dirname(manifest_path), manifest.entrypoint)

def _get_state():
    if (None not in (_process, _receiver)) and (_state.get('state', None) in (None, PENDING, RUNNING)):
        try:
            while _receiver.poll():
                update = _receiver.recv()
                _state.setdefault('log', []).extend(update.pop('log', []))
                _state.update(update)
        except EOFError as e:
            print(e) # TODO: handle better

    ret_val = copy.deepcopy(_state)
    # CPS-952: purge old log messages.
    # TODO: _state and its contents should be protected against multiple access.
    if 'log' in _state:
        del _state['log'][:]

    return ret_val

def _handle_failed_child_process(sender):
    while True:
        # Attempt to reap an exited child process.
        try:
            pid, exit_code = os.waitpid(-1, os.WNOHANG)
        except OSError:
            break

        # If the child process is the model process and it hasn't completed
        # properly, flag a failure.
        if (pid == _process.pid) and not _model_complete.value:
            sender.send({
                'state': FAILED,
                'message': 'Model process terminated abnormally with exit code {}.'.format(exit_code)
            })
        elif pid == 0:
            break

@app.route('/', methods=['GET'])
def _get_root():
    return jsonify(_get_state())

@app.route('/', methods=['POST'])
def _post_root():
    global _process, _receiver

    if _process is not None:
        return make_response(jsonify({ 'error': 'Cannot submit new job - job already running.' }), 409)

    job_request = request.get_json(force=True, silent=True) or {}
    try:
        model_id = job_request['modelId']
    except KeyError:
        return make_response(jsonify({'error': 'Required property "modelId" is missing.'}), 400)

    manifest, entrypoint = _load_entrypoint(app.config['model_path'])

    try:
        model = manifest.models[model_id]
    except KeyError:
        return make_response(jsonify({'error': 'Unknown model "{}".'.format(model_id)}), 500)

    missing_ports = [port.name for port in model.ports if port.required and (port.name not in job_request.get('ports', {}))]
    if missing_ports:
        return make_response(jsonify({'error': 'Missing bindings for required port(s): {}'.format(','.join(missing_ports))}), 500)

    args = app.config.get('args', {})
    runtime_type = _determine_runtime_type(entrypoint, args)

    _root_logger.setLevel(log_levels.to_stdlib_levelno(args.get('log_level', 'INFO')))

    _receiver, sender = multiprocessing.Pipe(False)

    #CS: Dsiabling for now to test graincast workflows.
    #signal.signal(signal.SIGCHLD, lambda sig, frame: _handle_failed_child_process(sender))

    _process = multiprocessing.Process(target=_JobProcess(entrypoint, manifest, runtime_type, args, job_request, sender, _logger))
    _process.start()

    return _get_root()

@app.route('/terminate', methods=['POST'])
def _post_terminate():
    args = request.get_json(force=True, silent=True) or {}
    timeout = args.get('timeout', 0.0)

    _logger.info('Received terminate message. Waiting %.2f seconds for model to terminate.', timeout)

    _process.terminate()
    _process.join(timeout)

    if _process.is_alive():
        _logger.warning('Model process failed to terminate within timeout. Sending SIGKILL.')
        os.kill(_process.pid, signal.SIGKILL)
    else:
        _logger.info('Model shut down cleanly.')

    if _state['state'] not in (COMPLETE, FAILED):
        _state['state'] = TERMINATED

    # Make best-effort attempt the web API.
    func = request.environ.get('werkzeug.server.shutdown')
    if callable(func):
        func()
    else:
        _logger.warning('Unable to terminate model API (shutdown hook unavailable).')

    return _get_root()
