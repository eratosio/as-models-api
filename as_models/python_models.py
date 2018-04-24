
from context import BasePort, BaseStreamPort, BaseMultistreamPort, BaseDocumentPort, BaseGridPort, BaseContext
from ports import STREAM_PORT, MULTISTREAM_PORT, DOCUMENT_PORT, GRID_PORT
import models
from util import resolve_service_config

from sensetdp.api import API
from sensetdp.auth import HTTPBasicAuth, HTTPKeyAuth

import importlib, os, requests, sys

def is_valid_entrypoint(entrypoint):
    entrypoint = os.path.abspath(entrypoint)
    
    return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.py')

def run_model(entrypoint, manifest, job_request, args, updater):
    model_id = job_request['modelId']
    model = manifest.models[model_id]
    
    # Load the model's module.
    model_dir, model_file = os.path.split(entrypoint)
    module_name, module_ext = os.path.splitext(model_file)
    sys.path.append(model_dir)
    module = importlib.import_module(module_name)
    
    # Locate a callable matching the model ID.
    try:
        implementation = models._models[model_id]
    except KeyError:
        implementation = getattr(module, model_id, None)
    if not callable(implementation):
        raise RuntimeError('Unable to locate callable "{}" in model "{}".'.format(model_id, entrypoint)) # TODO: more specific exception type?
    
    # Run the callable.
    implementation(Context(model, job_request, args, updater))

def session_for_auth(auth, verify=None):
    from requests import Session
    
    session = Session()
    session.verify = verify
    
    if isinstance(auth, HTTPKeyAuth):
        session.params[auth.header] = auth.key
    else:
        session.auth = auth
    
    return session

class PythonPort(BasePort):
    def __init__(self, context, port, binding):
        super(PythonPort, self).__init__(context, port)
        
        self._was_supplied = binding is not None
        self._binding = binding or {}
    
    @property
    def was_supplied(self):
        return self._was_supplied

class StreamPort(PythonPort, BaseStreamPort):
    @property
    def stream_id(self):
        return self._binding.get('streamId')

class MultistreamPort(PythonPort, BaseMultistreamPort):
    @property
    def stream_ids(self):
        return self._binding.get('streamIds')

class DocumentPort(PythonPort, BaseDocumentPort):
    @property
    def value(self):
        return self._binding.get('document')
    
    @value.setter
    def value(self, value):
        if value != self.value:
            self._binding['document'] = value
            self._context.update(modified_documents={ self.name: self.value })

class GridPort(PythonPort, BaseGridPort):
    @property
    def catalog_url(self):
        return self._binding['catalog']
    
    @property
    def dataset_path(self):
        return self._binding['dataset']

class _SCApiProxy(API):
    def __init__(self, context, auth, host, api_root, verify=None):
        self._context = context
        
        super(_SCApiProxy, self).__init__(auth, host=host, api_root=api_root, verify=verify)
    
    def create_observations(self, results, streamid):
        super(_SCApiProxy, self).create_observations(results, streamid=streamid)

class Context(BaseContext):
    _port_type_map = {
        STREAM_PORT: StreamPort,
        MULTISTREAM_PORT: MultistreamPort,
        DOCUMENT_PORT: DocumentPort,
        GRID_PORT: GridPort
    }
    
    def __init__(self, model, job_request, args, updater):
        super(Context, self).__init__()
        
        self._model_id = job_request['modelId']
        self._updater = updater
        self._debug = args.get('debug', False) or job_request.get('debug', False)
        
        bindings = job_request.get('ports', {})
        for port in model.ports:
            try:
                port_type = Context._port_type_map[port.type]
                self.ports._add(port_type(self, port, bindings.get(port.name)))
            except KeyError:
                raise ValueError('Unsupported port type "{}"'.format(port.type))
        
        self._sensor_config = job_request.get('sensorCloudConfiguration')
        self._analysis_config = job_request.get('analysisServicesConfiguration')
        self._thredds_config = job_request.get('threddsConfiguration')
        self._thredds_upload_config = job_request.get('threddsUploadConfiguration')
        
        self._sensor_client = self._analysis_client = self._thredds_client = self._thredds_upload_client = None
    
    def update(self, *args, **kwargs): # TODO: fix method signature
        self._updater.update(*args, **kwargs)
    
    @property
    def model_id(self):
        return self._model_id
    
    @property
    def sensor_client(self):
        if self._sensor_client is None and self._sensor_config is not None:
            _, host, api_root, auth, verify = resolve_service_config(**self._sensor_config)
            self._sensor_client = _SCApiProxy(self, auth, host, api_root, verify)
        
        return self._sensor_client
    
    @property
    def analysis_client(self):
        if self._analysis_client is None and self._analysis_config is not None:
            from as_client import Client as ASClient
            
            url, _, _, auth, verify = resolve_service_config(**self._analysis_config)
            self._analysis_client = ASClient(url, session=session_for_auth(auth, verify))
        
        return self._analysis_client
    
    @property
    def thredds_client(self):
        if self._thredds_client is None and self._thredds_config is not None:
            from tds_client import Client
            
            url, _, _, auth, verify = resolve_service_config(**self._thredds_config)
            
            self._thredds_client = Client(url, session_for_auth(auth, verify))
        
        return self._thredds_client
    
    @property
    def thredds_upload_client(self):
        if self._thredds_upload_client is None and self._thredds_upload_config is not None:
            from tds_upload import Client
            
            url, _, _, auth, verify = resolve_service_config(**self._thredds_upload_config)
            
            self._thredds_upload_client = Client(url, session_for_auth(auth, verify))
        
        return self._thredds_upload_client
    
    @property
    def debug(self):
        return self._debug
