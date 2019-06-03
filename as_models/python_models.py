
from .context import BaseContext
from .ports import STREAM_PORT, MULTISTREAM_PORT, DOCUMENT_PORT, GRID_PORT, STREAM_COLLECTION_PORT, DOCUMENT_COLLECTION_PORT, GRID_COLLECTION_PORT, OUTPUT_PORT
from . import models
from .util import resolve_service_config, urlparse

from senaps_sensor.api import API

from tds_client.catalog import Catalog
from tds_client.catalog.search import QuickSearchStrategy
from tds_client.util import urls

import importlib, os, sys

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
    updater.update() # Marks the job as running.
    implementation(Context(model, job_request, args, updater))

def session_for_auth(auth, verify=None):
    from requests import Session

    session = Session()
    session.verify = verify
    session.auth = auth

    return session

def urlpath(url):
    return urls.urlparse(url).path

class PythonPort(object):
    def __init__(self, context, port, binding):
        self.__context = context
        self.__port = port
        self._was_supplied = binding is not None
        self._binding = binding or {}

    @property
    def was_supplied(self):
        return self._was_supplied

    @property
    def type(self):
        return self.__port.type

    @property
    def name(self):
        return self.__port.name

    @property
    def direction(self):
        return self.__port.direction

    @property
    def _context(self):
        return self.__context

class StreamPort(PythonPort):
    def get(self, default=None):
        return self.stream_id if self.was_supplied else default

    @property
    def stream_id(self):
        return self._binding.get('streamId')

class MultistreamPort(PythonPort):
    def get(self, default=None):
        return self.stream_ids if self.was_supplied else default

    @property
    def stream_ids(self):
        return self._binding.get('streamIds')

class DocumentPort(PythonPort):
    def get(self, default=None):
        return self.value if self.was_supplied else default

    @property
    def document_id(self):
        return self._binding.get('documentId')

    @property
    def value(self):
        return self._binding.get('document')

    @value.setter
    def value(self, value):
        if value != self.value:
            self._binding['document'] = value

            mod_doc = {'documentId': self.document_id, 'document': self.value}

            if getattr(self, 'index', None) is not None:
                mod_doc['index'] = self.index

            self._context.update(modified_documents={ self.name: mod_doc })

class GridPort(PythonPort):
    def __init__(self, context, port, binding):
        super(GridPort, self).__init__(context, port, binding)

        self.__dataset = None

    def get(self, default=None):
        return self.dataset if self.was_supplied else default

    def upload_data(self, data, path=None, client=None, *args, **kwargs):
        # The port MUST be an output port.
        if self.direction != OUTPUT_PORT:
            raise ValueError('The "upload data" operation is only valid for output ports.')

        # If client not supplied, use the context's client.
        if client is None:
            client = self._context.thredds_upload_client
            if client is None:
                raise ValueError('No data upload client configured.')

            # If the port's catalog URL is supplied AND the context has a TDS
            # client configured, then the catalog URLs MUST match (since it is
            # assumed the upload client is configured to upload to the same TDS
            # server).
            client_catalog_url = getattr(self._context.thredds_client, 'catalog_url', None)
            if self.catalog_url != client_catalog_url:
                raise ValueError('Data may only be uploaded to the environment\'s own TDS server.')

        path = path or self.dataset_path

        client.upload_data(data, path, *args, **kwargs)

    @property
    def dataset(self):
        if self.__dataset is None:
            from tds_client import Catalog, Dataset

            client = self._context._get_thredds_client(self.catalog_url)

            if client is not None:
                catalog = Catalog(self.catalog_url, client)
                self.__dataset = Dataset(catalog, self.dataset_path)

        return self.__dataset

    @property
    def catalog_url(self):
        return self._binding.get('catalog')

    @property
    def dataset_path(self):
        return self._binding.get('dataset')

class CollectionPort(PythonPort):
    def __init__(self, context, port, binding, ports):
        super(CollectionPort, self).__init__(context, port, binding)

        for idx, p in enumerate(ports): # necessary to map back to response...
            ports[idx].index = idx

        self.__ports = ports

    def get(self, default=None):
        return self.__ports if self.was_supplied else (default or [])

    def __getitem__(self, i):
        return self.get()[i]

    def __len__(self):
        return len(self.get())

    def __iter__(self):
        return iter(self.get())

    def __str__(self):
        return ','.join(map(str, self.get()))

    def __repr__(self):
        return repr(self.get())

class _SCApiProxy(API):
    def __init__(self, context, auth, host, api_root, verify=None):
        self._context = context

        super(_SCApiProxy, self).__init__(auth, host=host, api_root=api_root, verify=verify, timeout=300, connect_retries=10, read_retries=10, status_retries=10)

    def create_observations(self, results, streamid):
        super(_SCApiProxy, self).create_observations(results, streamid=streamid)


class SenapsSearchStrategy(QuickSearchStrategy):
    def get_next_candidates(self, catalog, dataset_url):
        catalogs = super(SenapsSearchStrategy, self).get_next_candidates(catalog, dataset_url)

        # Check if one of the catalogs is the Senaps master org catalog.
        org_catalog = next((cat for cat in catalogs if 'org-catalogs.xml' == urls.path.basename(urlpath(cat.url))), None)
        if org_catalog:
            # Move the org catalog to the end of the list.
            catalogs.remove(org_catalog)
            catalogs.append(org_catalog)

            # Remove leading separators from the dataset path (it should always be treated as a relative path).
            dataset_path = dataset_url.lstrip(urls.path.sep)

            # Assume the dataset in question is an org dataset, and add its expected catalog to the list.
            orgs_base = urls.path.join(urls.path.dirname(urlpath(org_catalog.url)), 'org_catalogs')
            catalog_path = urls.path.join(urls.path.dirname(dataset_path), 'catalog.xml')
            catalog_url = urls.override(org_catalog.url, path=urls.path.join(orgs_base, catalog_path))
            catalogs.insert(0, Catalog(catalog_url, org_catalog.client))

        return catalogs

class Context(BaseContext):
    _port_type_map = {
        STREAM_PORT: StreamPort,
        MULTISTREAM_PORT: MultistreamPort,
        DOCUMENT_PORT: DocumentPort,
        GRID_PORT: GridPort,
        STREAM_COLLECTION_PORT: StreamPort,
        DOCUMENT_COLLECTION_PORT: DocumentPort,
        GRID_COLLECTION_PORT: GridPort
    }

    @staticmethod
    def is_collection_port(porttype):
        return porttype == STREAM_COLLECTION_PORT or porttype == GRID_COLLECTION_PORT or porttype == DOCUMENT_COLLECTION_PORT

    def __init__(self, model, job_request, args, updater):
        super(Context, self).__init__()

        self._model_id = job_request['modelId']
        self._updater = updater
        self._debug = args.get('debug', False) or job_request.get('debug', False)

        bindings = job_request.get('ports', {})
        for port in model.ports:
            try:
                binding = bindings.get(port.name, None)

                if self.is_collection_port(port.type):
                    binding_ports = binding.get('ports', []) if binding else []
                    inner_ports = [Context._port_type_map[port.type](self, port, inner_binding) for inner_binding in binding_ports]
                    self.ports._add(CollectionPort(self, port, binding, inner_ports))
                else:
                    port_type = Context._port_type_map[port.type]
                    self.ports._add(port_type(self, port, binding))

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

            self._thredds_client = Client(url, session_for_auth(auth, verify), strategy=SenapsSearchStrategy)

        return self._thredds_client

    @property
    def thredds_upload_client(self):
        if self._thredds_upload_client is None and self._thredds_upload_config is not None:
            from tdm import Client

            url, _, _, auth, verify = resolve_service_config(**self._thredds_upload_config)

            self._thredds_upload_client = Client(url, session_for_auth(auth, verify))

        return self._thredds_upload_client

    @property
    def debug(self):
        return self._debug
