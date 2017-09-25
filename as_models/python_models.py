
from context import BaseStreamPort, BaseMultistreamPort, BaseDocumentPort, BaseGridPort, BaseContext
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
    context = Context(manifest, job_request, args, updater)
    implementation(context)

class StreamPort(BaseStreamPort):
    def __init__(self, context, **kwargs):
        self._stream_id = kwargs.pop('streamId')
        
        super(StreamPort, self).__init__(context, **kwargs)
    
    @property
    def stream_id(self):
        return self._stream_id
    
    @property
    def was_supplied(self):
        return self._stream_id is not None

class MultistreamPort(BaseMultistreamPort):
    def __init__(self, context, **kwargs):
        self._stream_ids = kwargs.pop('streamIds')
        
        super(MultistreamPort, self).__init__(context, **kwargs)
    
    @property
    def stream_ids(self):
        return self._stream_ids
    
    @property
    def was_supplied(self):
        return self._stream_ids is not None

class DocumentPort(BaseDocumentPort):
    def __init__(self, context, **kwargs):
        self._value = kwargs.pop('document', None)
        self._supplied = self._value is not None
        
        super(DocumentPort, self).__init__(context, **kwargs)
    
    @property
    def value(self):
        return self._value
    
    @value.setter
    def value(self, value):
        if value != self._value:
            self._value = value
            self._context.update(modified_documents={ self.name: self.value })
    
    @property
    def was_supplied(self):
        return self._supplied

class GridPort(BaseGridPort):
    def __init__(self, context, **kwargs):
        self._catalog_url = kwargs.pop('catalog', None)
        self._dataset_path = kwargs.pop('dataset', None)
        
        super(GridPort, self).__init__(context, **kwargs)
    
    @property
    def catalog_url(self):
        return self._catalog_url
    
    @property
    def dataset_path(self):
        return self._dataset_path
    
    @property
    def was_supplied(self):
        return self._dataset_path is not None

class _SCApiProxy(API):
    def __init__(self, context, auth, host, api_root):
        self._context = context
        
        super(_SCApiProxy, self).__init__(auth, host=host, api_root=api_root)
    
    def create_observations(self, results, streamid):
        super(_SCApiProxy, self).create_observations(results, streamid=streamid)

class Context(BaseContext):
    def __init__(self, manifest, job_request, args, updater):
        super(Context, self).__init__()
        
        self._model_id = job_request['modelId']
        self._updater = updater
        self._debug = args.get('debug', False) or job_request.get('debug', False)
        
        # Add supplied ports.
        for name,v in job_request.get('ports', {}).iteritems():
            type = v.get('type')
            
            if type == STREAM_PORT:
                self.ports._add(StreamPort(self, name=name, **v))
            elif type == MULTISTREAM_PORT:
                self.ports._add(MultistreamPort(self, name=name, **v))
            elif type == DOCUMENT_PORT:
                self.ports._add(DocumentPort(self, name=name, **v))
            elif type == GRID_PORT:
                self.ports._add(GridPort(self, name=name, **v))
            else:
                raise ValueError('Unsupported port type "{}"'.format(type))
        
        # TODO: Add unsupplied ports.
        
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
            _, host, api_root, auth = resolve_service_config(**self._sensor_config)
            self._sensor_client = _SCApiProxy(self, auth, host, api_root)
        
        return self._sensor_client
    
    @property
    def analysis_client(self):
        if self._analysis_client is None and self._analysis_config is not None:
            from as_client import Client as ASClient
            
            url, _, _, auth = resolve_service_config(**self._analysis_config)
            self._analysis_client = ASClient(url, auth)
        
        return self._analysis_client
    
    @property
    def thredds_client(self):
        if self._thredds_client is None and self._thredds_config is not None:
            from tds_client import Client
            
            url, _, _, auth = resolve_service_config(**self._thredds_config)
            
            session = requests.Session()
            session.auth = auth
            
            self._thredds_client = Client(url, session)
        
        return self._thredds_client
    
    @property
    def thredds_upload_client(self):
        if self._thredds_upload_client is None and self._thredds_upload_config is not None:
            from tds_upload import Client
            
            url, _, _, auth = resolve_service_config(**self._thredds_upload_config)
            
            session = requests.Session()
            session.auth = auth
            
            self._thredds_upload_client = Client(url, session)
        
        return self._thredds_upload_client
    
    @property
    def debug(self):
        return self._debug
