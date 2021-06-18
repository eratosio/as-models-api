
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
_SUBPROCESS_STARTUP_TIME_LIMIT = 30.0  # seconds
_ABNORMAL_TERMINATION_GRACE_PERIOD = 5.0  # seconds


logging.captureWarnings(True)


def _signalterm_handler(signum, stack):
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

    def update(self, message=_SENTINEL, progress=_SENTINEL, modified_streams=None, modified_documents=None):
        if modified_streams is not None:
            warnings.warn('Usage of modified_streams argument is deprecated and will be removed in a future version',
                          DeprecationWarning)
        if modified_documents is not None:
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

    def log(self, message, level=None, file=None, line=None, timestamp=None, logger_=None):
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
            'logger': logger_
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
    def __init__(self, entrypoint, manifest, runtime_type, args, job_request, sender, logger_):
        self._entrypoint = entrypoint
        self._manifest = manifest
        self._runtime_type = runtime_type
        self._args = args
        self._job_request = job_request
        self._sender = sender
        self._logger = logger_

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
        process_logger = logging.getLogger('JobProcess')

        # Run the model!
        try:
            # TODO: see if the runtime can be made more dynamic
            model_id = self._job_request['modelId']
            process_logger.debug('Calling implementation method for model %s...', model_id)
            if self._runtime_type == 'python':
                python_models.run_model(self._entrypoint, self._manifest, self._job_request, self._args, updater)
            elif self._runtime_type == 'r':
                r_models.run_model(self._entrypoint, self._manifest, self._job_request, self._args, updater)
            else:
                raise ValueError('Unsupported runtime type "{}".'.format(self._runtime_type))
            _model_complete.value = 1
            process_logger.debug('Implementation method for model %s returned.', model_id)

            self._sender.send({
                'state': COMPLETE,
                'progress': 1.0
            })
        except BaseException as e:
            process_logger.critical('Model failed with exception: %s', e)

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


class ApiState(object):
    def __init__(self):
        self.process = None
        self.receiver = None
        self.model_id = None
        self.state = None
        self.model_complete = None
        self.started_timestamp = None
        self.failed_timestamp = None
        self._root_logger = None

        self.reset()

    def reset(self):
        self.process = self.receiver = self.model_id = self.started_timestamp = self.failed_timestamp = None

        self.state = {'state': PENDING}
        self.model_complete = multiprocessing.Value('i', 0)

        self._root_logger = logging.getLogger()
        self._root_logger.addHandler(_WebAPILogHandler(api_state.state))

    def check_subprocess_startup_timeout(self):
        if self.subprocess_running:
            return True

        now = time.time()
        if self.started_timestamp is None:
            self.started_timestamp = now

        return (now - self.started_timestamp) < _SUBPROCESS_STARTUP_TIME_LIMIT

    def check_failure_cleanup_timeout(self):
        now = time.time()
        if self.failed_timestamp is None:
            self.failed_timestamp = now

        return (now - self.failed_timestamp) < _ABNORMAL_TERMINATION_GRACE_PERIOD

    def fail_with_exception(self, message, dev_message, data):
        self.state['state'] = FAILED
        self.state['exception'] = {
            'developer_msg': dev_message,
            'msg': message,
            'data': sanitize_dict_for_json(data),
            'model_id': self.model_id
        }

    def set_log_level(self, level):
        self._root_logger.setLevel(level)

    @property
    def model_state(self):
        return self.state.get('state')

    @model_state.setter
    def model_state(self, state):
        self.state['state'] = state

    @property
    def subprocess_starting(self):
        return (self.process is not None) and not self.process.is_alive() and (self.model_state == PENDING)

    @property
    def subprocess_running(self):
        return (self.process is not None) and self.process.is_alive()

    @property
    def model_running(self):
        return self.subprocess_running and (self.model_state not in (COMPLETE, FAILED, TERMINATED))


app = Flask(__name__)

api_state = ApiState()

logger = logging.getLogger('WebAPI')
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
    state = api_state.model_state

    if (None not in (api_state.process, api_state.receiver)) and (state in (None, PENDING, RUNNING)):
        try:
            while api_state.receiver.poll():
                update = api_state.receiver.recv()
                api_state.state.setdefault('log', []).extend(update.pop('log', []))
                api_state.state.update(update)
        except EOFError as e:
            print(e)  # TODO: handle better

    _check_for_abnormal_termination()

    ret_val = copy.deepcopy(api_state.state)
    # CPS-952: purge old log messages.
    if 'log' in api_state.state:
        purge_count = len(api_state.state['log'])
        for i in range(purge_count, 0, -1):
            # count backwards always deleting the 0th item.
            # allows us to avoid clobbering incoming messages while we work.
            del api_state.state['log'][0]

    ret_val['api_version'] = __version__

    if api_state.subprocess_running:
        try:
            ret_val['stats'] = {
                'peakMemoryUsage': get_peak_memory_usage(api_state.process.pid)
            }
        except Exception:
            pass  # Only make a best-effort attempt to get stats. Don't let it kill the API.

    return ret_val


def _check_for_abnormal_termination():
    # No need to worry if the model is running, or if it's still starting and the startup timeout hasn't elapsed.
    if api_state.subprocess_running and api_state.check_subprocess_startup_timeout():
        api_state.failed_timestamp = None
        return

    # Continue running for a moment to allow any pending messages from the model to come through the IPC pipe.
    if api_state.check_failure_cleanup_timeout():
        return

    # At this point, the model must have started or terminated abnormally and the grace period elapsed. Attempt to clean
    # up the defunct process.
    failure_mode = 'failed to start' if api_state.subprocess_starting else 'terminated abnormally'
    message = 'Model "{}" {}.'.format(api_state.model_id, failure_mode)
    logger.critical('{} Waiting {} seconds for process cleanup.'.format(message, _ABNORMAL_TERMINATION_GRACE_PERIOD))
    api_state.process.join(_ABNORMAL_TERMINATION_GRACE_PERIOD)

    if api_state.process.exitcode is None:
        logger.error('Failed to clean up model process.')
    elif api_state.process.exitcode < 0:
        logger.info('Process successfully cleaned up, exit code was {}.'.format(api_state.process.exitcode))

    api_state.fail_with_exception(
        message,
        'Model "{}" terminated with exit code {}.'.format(api_state.model_id, api_state.process.exitcode),
        {'exitCode': api_state.process.exitcode}
    )


def terminate(timeout=0.0):
    if api_state.process is None:
        return  # Can't terminate model - it never started.

    logger.debug('Waiting %.2f seconds for model to terminate.', timeout)

    api_state.process.terminate()
    api_state.process.join(timeout)

    if api_state.process.is_alive():
        logger.warning('Model process failed to terminate within timeout. Sending SIGKILL.')
        os.kill(api_state.process.pid, signal.SIGKILL)
    else:
        logger.debug('Model shut down cleanly.')

    if api_state.model_state not in (COMPLETE, FAILED):
        api_state.model_state = TERMINATED

    # Make best-effort attempt to stop the web API.
    func = request.environ.get('werkzeug.server.shutdown')
    if callable(func):
        func()
    else:
        logger.warning('Unable to terminate model API (shutdown hook unavailable).')


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
    if api_state.process is not None:
        return make_response(jsonify(_get_state()), 409)

    api_state.reset()

    job_request = request.get_json(force=True, silent=True) or {}

    try:
        api_state.model_id = job_request['modelId']
    except KeyError:
        return make_response(jsonify({'error': 'Required property "modelId" is missing.'}), 400)

    manifest, entrypoint = _load_entrypoint(app.config['model_path'])

    try:
        model = manifest.models[api_state.model_id]
    except KeyError:
        return make_response(jsonify({'error': 'Unknown model "{}".'.format(api_state.model_id)}), 500)

    missing_ports = [port.name for port in model.ports if
                     port.required and (port.name not in job_request.get('ports', {}))]

    if missing_ports:
        logger.warning('Missing bindings for required model port(s): {}'.format(', '.join(missing_ports)))

    args = app.config.get('args', {})
    runtime_type = _determine_runtime_type(entrypoint, args)

    api_state.set_log_level(log_levels.to_stdlib_levelno(args.get('log_level', 'INFO')))

    api_state.receiver, sender = multiprocessing.Pipe(False)
    job_process = _JobProcess(entrypoint, manifest, runtime_type, args, job_request, sender, logger)

    try:
        with multiprocessing.get_context('spawn') as mp:
            api_state.process = mp.Process(target=job_process)
    except (AttributeError, ValueError):
        # AttributeError if running pre-3.4 Python (and get_context() is therefore unavailable); ValueError if "spawn"
        # is unsupported.
        api_state.process = multiprocessing.Process(target=job_process)

    api_state.process.start()

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

    api_state.fail_with_exception(message, dev_message, data)

    return make_response(_get_root(), 500)
