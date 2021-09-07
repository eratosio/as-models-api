
from __future__ import absolute_import

import uuid
import logging

from as_models.ports import STREAM_COLLECTION_PORT, GRID_COLLECTION_PORT, DOCUMENT_COLLECTION_PORT, STREAM_PORT, \
    MULTISTREAM_PORT, DOCUMENT_PORT, GRID_PORT
from as_models.context import BaseContext
from as_models.python_models import StreamPort, MultistreamPort, DocumentPort, GridPort, Context, CollectionPort
from as_models.util import resolve_service_config, urlparse, urljoin
from as_models.manifest import Port

from senaps_sensor.api import API


class _SCApiProxy(API):  # TODO: see if there is a neat way to declare this lazily.
    def __init__(self, context, auth, host, api_root, verify=True):
        self._context = context

        super(_SCApiProxy, self).__init__(auth, host=host, api_root=api_root, verify=verify)

    def create_observations(self, results, streamid):
        super(_SCApiProxy, self).create_observations(results, streamid=streamid)

        self._context.update(modified_streams=[streamid])


def _generate_binding(required_val, **kwargs):
    return None if required_val is None else kwargs


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

    def __init__(self, model_id=None):
        super(Context, self).__init__()

        self._model_id = model_id

        self._modified_streams = set()
        self._modified_documents = {}

        self._sensor_config = self._analysis_config = self._thredds_config = self._thredds_upload_config = None
        self._sensor_client = self._analysis_client = self._thredds_client = self._thredds_upload_client = None

    @staticmethod
    def is_collection_port(porttype):
        return porttype == STREAM_COLLECTION_PORT or porttype == GRID_COLLECTION_PORT or porttype == DOCUMENT_COLLECTION_PORT

    def configure_port(self, name, type, direction,
                       stream_id=None, stream_ids=None,
                       value=None, values=None,
                       document_id=None, document_ids=None,
                       catalog_url=None, dataset_path=None,
                       catalog_urls=None, dataset_paths=None,
                       doc_organisation_id=None, doc_group_ids=None):
        """
        Configure a model port for testing. This method allows port to be defined
        with appropriate mappings.
        The value property can be used to provide an initial value for document port mappings. If an Analysis Service
        client, doc_organisation_id and doc_group_id are also available the document will be pushed to the configured
        Analysis Service. A mock Analysis Service is available in the testing.mock module.

        """
        port = Port({'portName': name, 'direction': direction, 'type': type, 'required': False})

        try:
            if self.is_collection_port(type):
                binding_ports = []

                if stream_ids:
                    binding_ports = [_generate_binding(stream_id, streamId=stream_id) for stream_id in stream_ids]

                if values:
                    binding_ports = [_generate_binding(value, document=value) for value in values]
                    for value in values:
                        gen_doc_id = str(uuid.uuid4())
                        self.initialise_document(gen_doc_id, value, doc_organisation_id, doc_group_ids)

                if document_ids:
                    binding_ports = [_generate_binding(document_id, documentId=document_id) for document_id in document_ids]

                if catalog_urls and dataset_paths:
                    binding_ports = [_generate_binding(catalog, catalog=catalog, dataset=dataset)
                                     for (catalog, dataset) in zip(catalog_urls, dataset_paths)]

                binding = {'ports': binding_ports}

                inner_ports = [Context._port_type_map[port.type](self, port, inner_binding) for inner_binding in binding_ports]
                self.ports._add(CollectionPort(self, port, binding, inner_ports))
            else:
                port_type = Context._port_type_map[type]

                binding = None

                if stream_id:
                    binding = _generate_binding(stream_id, streamId=stream_id)

                if value is not None:
                    gen_doc_id = str(uuid.uuid4())
                    binding = _generate_binding(value, document=value, documentId=gen_doc_id)
                    self.initialise_document(gen_doc_id, value, doc_organisation_id, doc_group_ids)

                if document_id:
                    binding = _generate_binding(document_id, documentId=document_id)

                if catalog_url and dataset_path:
                    binding = _generate_binding(dataset_path, catalog=catalog_url, dataset=dataset_path)

                self.ports._add(port_type(self, port, binding))

        except KeyError:
            raise ValueError('Unsupported port type "{}"'.format(port.type))

        return self

    def initialise_document(self, document_id, value, organisation_id, group_ids):
        """
        Initialise document in configured Analysis Service.
        """

        if not self.analysis_client:
            return

        if group_ids is None:
            group_ids = []

        if organisation_id:
            from as_client import Document
            document = Document()
            document.id = document_id
            document.organisation_id = organisation_id
            document.group_ids = group_ids
            self.analysis_client.set_document_value(document, value=value)

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
