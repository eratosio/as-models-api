
from __future__ import print_function

import copy
import datetime
import json
import logging
import multiprocessing
import os
import signal
import sys
import time
import traceback
from flask import Flask, jsonify, make_response, request
import warnings

from werkzeug.exceptions import InternalServerError

from . import log_levels, python_models, r_models
from .exceptions import SenapsModelError
from .manifest import Manifest
from .model_state import PENDING, RUNNING, COMPLETE, TERMINATED, FAILED
from .sentinel import Sentinel
from .stats import get_peak_memory_usage
from .util import sanitize_dict_for_json
from .version import __version__

_SENTINEL = Sentinel()
_ABNORMAL_TERMINATION_GRACE_PERIOD = 5.0  # seconds


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

    def update(self, message=_SENTINEL, progress=_SENTINEL, modified_streams=[], modified_documents={}):
        if modified_streams:
            warnings.warn('Usage of modified_streams argument is deprecated and will be removed in a future version',
                          DeprecationWarning)
        if modified_documents:
            warnings.warn('Usage of modified_documents argument is deprecated and will be removed in a future version',
                          DeprecationWarning)

        update = {k: v for k, v in {
            'state': RUNNING,
            'message': message,
            'progress': progress
        }.items() if v not in (_SENTINEL, self._state.get(k, _SENTINEL))}

        if update:
            self._state.update(update)
            self._sender.send(update)

    def log(self, message, level=None, file=None, line=None, timestamp=None, logger=None):
        if level is not None and level not in log_levels.LEVELS:
            raise ValueError(
                'Unsupported log level "{}". Supported values: {}'.format(level, ', '.join(log_levels.LEVELS)))

        message = message.rstrip() if message else None

        if not message:
            return

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

        self._sender.send({'log': [log_entry]})


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
            timestamp=time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)) + '.{:03}Z'.format(
                int(record.msecs) % 1000),
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

    def __post_exception(self, exc, model_id):
        """
        Given an exception object, use the _sender to post a dict of results.

        :param exc: Exception or subclass thereof
        :param model_id: str: model id that raised this exception. May be None.
        :return: None
        """
        # get formatted traceback.
        tb = sys.exc_info()[-1]
        developer_msg = ''
        if tb is not None:
            # it should only be able to be None if another exception is raised on this thread
            # or someone invoked exc_clear prior to us consuming it.
            developer_msg = ''.join(traceback.format_exception(etype=type(exc), value=exc, tb=tb))
        user_data = sanitize_dict_for_json(exc.user_data) if type(exc) == SenapsModelError else None
        msg = exc.msg if type(exc) == SenapsModelError else str(exc)
        self._sender.send({
            'state': FAILED,
            'exception': {  # CPS-889: this format only supported since AS-API v3.9.3
                'developer_msg': developer_msg,
                'msg': msg,
                'data': user_data,
                'model_id': model_id
            }
        })

    def __call__(self):
        model_id = None  # pre-declare this so the name exists later if an exception occurs.
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

            self._sender.send({
                'state': COMPLETE,
                'progress': 1.0
            })
        except BaseException as e:
            logger.critical('Model failed with exception: %s', e)

            self.__post_exception(e, model_id)


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
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)) + '.{:03}Z'.format(
                int(record.msecs) % 1000),
            'logger': record.name
        })


app = Flask(__name__)

_process = _receiver = _state = _model_complete = _root_logger = _logger = _model_id = _failed_timestamp = None


def _reset():
    global _process, _receiver, _state, _model_complete, _root_logger, _logger

    _process = _receiver = None

    _state = {'state': PENDING}
    _model_complete = multiprocessing.Value('i', 0)

    _root_logger = logging.getLogger()
    _root_logger.addHandler(_WebAPILogHandler(_state))

    _logger = logging.getLogger('WebAPI')


_reset()

logging.getLogger('werkzeug').setLevel(logging.ERROR)  # disable unwanted Flask HTTP request logs


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
    state = _state.get('state', None)

    if (None not in (_process, _receiver)) and (state in (None, PENDING, RUNNING)):
        try:
            while _receiver.poll():
                update = _receiver.recv()
                _state.setdefault('log', []).extend(update.pop('log', []))
                _state.update(update)
        except EOFError as e:
            print(e)  # TODO: handle better

    _check_for_abnormal_termination()

    ret_val = copy.deepcopy(_state)
    # CPS-952: purge old log messages.
    if 'log' in _state:
        purge_count = len(_state['log'])
        for i in range(purge_count, 0, -1):
            # count backwards always deleting the 0th item.
            # allows us to avoid clobbering incoming messages while we work.
            del _state['log'][0]

    ret_val['api_version'] = __version__

    if (_process is not None) and _process.is_alive():
        try:
            ret_val['stats'] = {
                'peakMemoryUsage': get_peak_memory_usage(_process.pid)
            }
        except Exception:
            pass  # Only make a best-effort attempt to get stats. Don't let it kill the API.

    return ret_val


def _check_for_abnormal_termination():
    global _failed_timestamp, _process, _state

    # Don't bother continuing if the model process hasn't started yet, is still running, or has already been marked
    # finished.
    if _process is None or _process.is_alive() or _state.get('state') in (COMPLETE, FAILED, TERMINATED):
        return

    # Record the time at which the model process is first detected to have failed.
    if _failed_timestamp is None:
        _failed_timestamp = time.time()

    # Allow up to _ABNORMAL_TERMINATION_GRACE_PERIOD seconds for any pending messages from the model to come through the
    # IPC pipe.
    if (time.time() - _failed_timestamp) < _ABNORMAL_TERMINATION_GRACE_PERIOD:
        return

    # At this point, the model must have terminated abnormally and the grace period elapsed. Attempt to clean up the
    # defunct process.
    _logger.critical('Model process has terminated abnormally. Waiting {} seconds for process cleanup.'.format(
        _ABNORMAL_TERMINATION_GRACE_PERIOD
    ))
    _process.join(_ABNORMAL_TERMINATION_GRACE_PERIOD)

    if _process.exitcode is None:
        _logger.error('Failed to clean up model process.')
    elif _process.exitcode < 0:
        _logger.info('Process successfully cleaned up, exit code was {}.'.format(_process.exitcode))

    _fail_with_exception(
        'Model "{}" terminated abnormally.'.format(_model_id),
        'Model "{}" terminated with exit code {}.'.format(_model_id, _process.exitcode),
        {
            'exitCode': _process.exitcode
        }
    )


def _fail_with_exception(message, dev_message, data):
    _state['state'] = FAILED
    _state['exception'] = {
        'developer_msg': dev_message,
        'msg': message,
        'data': sanitize_dict_for_json(data),
        'model_id': _model_id
    }


def terminate(timeout=0.0):
    _logger.debug('Waiting %.2f seconds for model to terminate.', timeout)

    _process.terminate()
    _process.join(timeout)

    if _process.is_alive():
        _logger.warning('Model process failed to terminate within timeout. Sending SIGKILL.')
        os.kill(_process.pid, signal.SIGKILL)
    else:
        _logger.debug('Model shut down cleanly.')

    if _state['state'] not in (COMPLETE, FAILED):
        _state['state'] = TERMINATED

    # Make best-effort attempt to stop the web API.
    func = request.environ.get('werkzeug.server.shutdown')
    if callable(func):
        func()
    else:
        _logger.warning('Unable to terminate model API (shutdown hook unavailable).')


def _get_traceback(exc):
    try:
        return ''.join(traceback.format_tb(exc.__traceback__))
    except AttributeError:
        pass


@app.route('/', methods=['GET'])
def _get_root():
    return jsonify(_get_state())


@app.route('/', methods=['POST'])
def _post_root():
    global _process, _receiver, _model_id

    if _process is not None and _process.is_alive():
        return make_response(jsonify({'error': 'Cannot submit new job - job already running.'}), 409)

    _reset()

    job_request = request.get_json(force=True, silent=True) or {}

    try:
        _model_id = job_request['modelId']
    except KeyError:
        return make_response(jsonify({'error': 'Required property "modelId" is missing.'}), 400)

    manifest, entrypoint = _load_entrypoint(app.config['model_path'])

    try:
        model = manifest.models[_model_id]
    except KeyError:
        return make_response(jsonify({'error': 'Unknown model "{}".'.format(_model_id)}), 500)

    missing_ports = [port.name for port in model.ports if
                     port.required and (port.name not in job_request.get('ports', {}))]

    if missing_ports:
        _logger.warning('Missing bindings for required model port(s): {}'.format(', '.join(missing_ports)))

    args = app.config.get('args', {})
    runtime_type = _determine_runtime_type(entrypoint, args)

    _root_logger.setLevel(log_levels.to_stdlib_levelno(args.get('log_level', 'INFO')))

    _receiver, sender = multiprocessing.Pipe(False)
    job_process = _JobProcess(entrypoint, manifest, runtime_type, args, job_request, sender, _logger)

    try:
        with multiprocessing.get_context('spawn') as mp:
            _process = mp.Process(target=job_process)
    except (AttributeError, ValueError):
        # AttributeError if running pre-3.4 Python (and get_context() is therefore unavailable); ValueError if "spawn"
        # is unsupported.
        _process = multiprocessing.Process(target=job_process)

    _process.start()

    return _get_root()


@app.route('/terminate', methods=['POST'])
def _post_terminate():
    args = request.get_json(force=True, silent=True) or {}
    timeout = args.get('timeout', 0.0)

    terminate(timeout)

    return _get_root()


@app.errorhandler(InternalServerError)
def handle_500(e):
    cause = getattr(e, "original_exception", e)

    message = 'An internal error occurred.'
    dev_message = str(cause)
    data = {'originalTraceback': _get_traceback(cause)}

    try:
        terminate(5.0)
    except Exception as termination_error:
        message += ' A further error occurred when attempting to terminate the model in response to the first error.'
        dev_message = 'Original error: ' + dev_message + '\nTermination error: ' + str(termination_error)
        data['terminationTraceback'] = _get_traceback(termination_error)

    _fail_with_exception(message, dev_message, data)

    return make_response(_get_root(), 500)
