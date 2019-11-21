
from .context import BaseContext
from .python_models import StreamPort, MultistreamPort, DocumentPort, GridPort, Context, CollectionPort
from .util import resolve_service_config
from . import ports
from .manifest import Port

from senaps_sensor.api import API

import contextlib, requests, warnings

try:
    from functools import partialmethod
except ImportError:
    # Python 2 fallback: https://gist.github.com/carymrobbins/8940382
    from functools import partial

    class partialmethod(partial):
        def __get__(self, instance, owner):
            if instance is None:
                return self

            return partial(self.func, instance, *(self.args or ()), **(self.keywords or {}))

@contextlib.contextmanager
def ssl_verification_disabled():
    old_request = requests.Session.request
    requests.Session.request = partialmethod(old_request, verify=False)

    warnings.filterwarnings('ignore', 'Unverified HTTPS request')
    yield
    warnings.resetwarnings()

    requests.Session.request = old_request

class _SCApiProxy(API): # TODO: see if there is a neat way to declare this lazily.
    def __init__(self, context, auth, host, api_root, verify=True):
        self._context = context

        super(_SCApiProxy, self).__init__(auth, host=host, api_root=api_root, verify=verify)

    def create_observations(self, results, streamid):
        super(_SCApiProxy, self).create_observations(results, streamid=streamid)

        self._context.update(modified_streams=[streamid])

def _generate_binding(required_val, **kwargs):
    return None if required_val is None else kwargs

class Context(BaseContext):
    def __init__(self, model_id=None):
        super(Context, self).__init__()

        self._model_id = model_id

        self._modified_streams = set()
        self._modified_documents = {}

        self._sensor_config = self._analysis_config = self._thredds_config = self._thredds_upload_config = None
        self._sensor_client = self._analysis_client = self._thredds_client = self._thredds_upload_client = None

    def configure_port(self, name, type, direction, stream_id=None, stream_ids=None, value=None, catalog_url=None, dataset_path=None):
        port = Port({'portName': name, 'direction': direction, 'type': type, 'required': False})

        try:
            if self.is_collection_port(port.type):
                binding_ports = []

                if stream_ids:
                    binding_ports = [_generate_binding(stream_id, stream_id=stream_id) for stream_id in stream_ids]

                binding = {'ports': binding_ports}

                inner_ports = [Context._port_type_map[port.type](self, port, inner_binding) for inner_binding in binding_ports]
                self.ports._add(CollectionPort(self, port, binding, inner_ports))
            else:
                port_type = Context._port_type_map[port.type]

                binding = None

                if stream_id:
                    binding = _generate_binding(stream_id, stream_id=stream_id)

                if value:
                    binding = _generate_binding(value, value=value)

                if catalog_url and dataset_path:
                    binding = _generate_binding(dataset_path, catalog=catalog_url, dataset=dataset_path)

                self.ports._add(port_type(self, port, binding))

        except KeyError:
            raise ValueError('Unsupported port type "{}"'.format(port.type))

        return self

    def configure_sensor_client(self, url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None, verify=True):
        self._sensor_config = resolve_service_config(url, scheme, host, api_root, port, username, password, api_key, verify=verify)
        return self

    def configure_analysis_client(self, url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None, verify=True):
        self._analysis_config = resolve_service_config(url, scheme, host, api_root, port, username, password, api_key, verify=verify)
        return self

    def configure_thredds_client(self, url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None, verify=True):
        self._thredds_config = resolve_service_config(url, scheme, host, api_root, port, username, password, api_key, verify=verify)
        return self

    def configure_thredds_upload_client(self, url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None, verify=True):
        self._thredds_upload_config = resolve_service_config(url, scheme, host, api_root, port, username, password, api_key, verify=verify)
        return self

    def configure_clients(self, url='', scheme=None, host=None, port=None, username=None, password=None, api_key=None, sensor_path=None, analysis_path=None, thredds_path=None, thredds_upload_path=None, verify=True):
        if sensor_path:
            self.configure_sensor_client(url, scheme, host, sensor_path, port, username, password, api_key, verify)
        if analysis_path:
            self.configure_analysis_client(url, scheme, host, analysis_path, port, username, password, api_key)
        if thredds_path:
            self.configure_thredds_client(url, scheme, host, thredds_path, port, username, password, api_key, verify)
        if thredds_upload_path:
            self.configure_thredds_upload_client(url, scheme, host, thredds_upload_path, port, username, password, api_key, verify)

    def update(self, message=None, progress=None, modified_streams=[], modified_documents={}):
        # TODO: figure out a good way of handling the "message" and "progress" parameters

        self._modified_streams.update(modified_streams)
        self._modified_documents.update(modified_documents)

    @property
    def modified_streams(self):
        return self._modified_streams

    @property
    def modified_documents(self):
        return self._modified_documents

    @property
    def model_id(self):
        return self._model

    @model_id.setter
    def model_id(self, model_id):
        self._model_id = model_id

    @property
    def sensor_client(self):
        if self._sensor_client is None and self._sensor_config is not None:
            _, host, api_root, auth, verify = self._sensor_config
            self._sensor_client = _SCApiProxy(self, auth, host, api_root, verify)

        return self._sensor_client

    @property
    def analysis_client(self):
        if self._analysis_client is None and self._analysis_config is not None:
            from as_client import Client

            url, _, _, auth, verify = self._analysis_config
            self._analysis_client = Client(url, auth=auth)

        return self._analysis_client

    @property
    def thredds_client(self):
        if self._thredds_client is None and self._thredds_config is not None:
            from tds_client import Client
            from requests import Session

            url, _, _, auth, verify = self._thredds_config

            session = Session()
            session.auth = auth
            session.verify = verify

            self._thredds_client = Client(url, session)

        return self._thredds_client

    @property
    def thredds_upload_client(self):
        if self._thredds_upload_client is None and self._thredds_upload_config is not None:
            from tdm import Client
            from requests import Session

            url, _, _, auth, verify = self._thredds_upload_config

            session = Session()
            session.auth = auth
            session.verify = verify

            self._thredds_upload_client = Client(url, session)

        return self._thredds_upload_client
