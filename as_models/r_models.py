
from .sentinel import Sentinel
from .util import resolve_service_config, session_for_auth

from as_client import Client
import os

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

# NOTE: this module makes frequent use of lazy imports to ensure rpy2 stuff is
# only imported on an as-needed basis.

_SENTINEL = Sentinel()


def is_valid_entrypoint(entrypoint):
    entrypoint = os.path.abspath(entrypoint)

    return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.r')


def run_model(entrypoint, manifest, job_request, args, updater):
    from rpy2.robjects import r, conversion
    from rpy2.rinterface import NULL
    from rpy2.robjects.vectors import ListVector

    model_id = job_request['modelId']
    model = manifest.models[model_id]
    # Load the model's module.
    r.source(entrypoint)

    # Locate a function matching the model ID.
    try:
        implementation = r[model_id]
    except LookupError:
        # TODO: more specific exception type?
        raise RuntimeError('Unable to locate function "{}" in model "{}".'.format(model_id,
                                                                                  entrypoint))
    if not callable(implementation):
        # TODO: more specific exception type?
        raise RuntimeError('Member "{}" of model "{}" is not a callable function.'.format(model_id,
                                                                                          entrypoint))

    # Enable custom conversions.
    # Requires rpy2==3.3.x (r_requirements.txt)
    @conversion.py2rpy.register(type(None))
    def convert_none(none):
        return NULL

    # We may need an Analysis Service client for some backwards-compatibility hacks.
    url, _, _, auth, verify = resolve_service_config(**job_request.get('analysisServicesConfiguration'))
    analysis_client = Client(url, session=session_for_auth(auth, verify))

    # Convert request to R-compatible.
    r_sensor_config = _convert_service_config(job_request.get('sensorCloudConfiguration'))
    r_analysis_config = _convert_service_config(job_request.get('analysisServicesConfiguration'))
    r_thredds_config = _convert_service_config(job_request.get('threddsConfiguration'))
    r_ports = _convert_ports(model.ports, job_request.get('ports', {}), analysis_client)
    r_update = _convert_update(updater.update, model, job_request.get('ports', {}), analysis_client)
    r_logger = _convert_logger(updater.log)

    # Create context object.
    context = {
        'ports': r_ports,
        'update': r_update,
        'log': r_logger
    }
    if r_sensor_config:
        context['sensor_config'] = r_sensor_config
    if r_analysis_config:
        context['analysis_config'] = r_analysis_config
    if r_thredds_config:
        context['thredds_config'] = r_thredds_config

    # Run the implementation.
    updater.update()  # Marks the job as running.
    implementation(ListVector(context))


def _convert_ports(model_ports, port_bindings, analysis_client):
    from rpy2.robjects.vectors import ListVector

    result = {}
    for port in model_ports:
        port_name = port.name
        port_config = port_bindings.get(port_name, {})

        # AS-API sends model requests that also include the 'direction' property.
        # as_models now uses the port direction from the manifest file.
        # Make sure this property is removed here to avoid conflict.
        if 'direction' in port_config:
            port_config.pop('direction')

        if 'ports' in port_config:
            inner_ports = port_config['ports']

            # to preserve order when returning results, inject the collection index
            for idx, iport in enumerate(inner_ports):
                iport['index'] = idx
                if 'direction' in iport:
                    print('dropping direction...')
                    iport.pop('direction')

            result[str(port_name)] = [_convert_port(i, analysis_client) for i in inner_ports]
        else:
            result[str(port_name)] = _convert_port(port_config, analysis_client)

    return ListVector(result)


def _convert_port(port, analysis_client):
    from rpy2.robjects.vectors import ListVector

    port_dict = {str(k): v for k, v in port.items()}

    # Backwards compatibility: if the port's a document port, pre-load its value from the supplied document ID.
    # TODO: deprecate this, and require R models to obtain the document value lazily.
    if 'document' not in port_dict and 'documentId' in port_dict:
        port_dict['document'] = analysis_client.get_document_value(port_dict['documentId'])

    return ListVector(port_dict)


def _convert_service_config(config):
    from rpy2.robjects.vectors import ListVector

    if config is not None:
        result = {'url': config.get('url', None)}

        if result['url'] is None:
            scheme = config.get('scheme', 'http')
            path = config.get('apiRoot', config.get('path', ''))
            netloc = config['host']
            if 'port' in config:
                netloc += ':{}'.format(config['port'])

            result['url'] = urlparse.urlunparse((scheme, netloc, path, '', '', ''))

        if result['url'][-1] != '/':
            result['url'] += '/'

        if 'apiKey' in config:
            result['api_key'] = config['apiKey']
        elif 'username' in config and 'password' in config:
            result['username'] = config['username']
            result['password'] = config['password']

        return ListVector(result)


def _convert_update(update, model, port_bindings, analysis_client):
    import rpy2.rinterface as ri
    from rpy2.robjects.vectors import Vector, ListVector

    def wrapper(message=_SENTINEL, progress=_SENTINEL, modified_streams=_SENTINEL, modified_documents=_SENTINEL):
        update_kwargs = {}

        if message not in (_SENTINEL, ri.NULL):
            update_kwargs['message'] = _extract_scalar(message)
        if progress not in (_SENTINEL, ri.NULL):
            update_kwargs['progress'] = _extract_scalar(progress)
        if modified_streams not in (_SENTINEL, ri.NULL):
            update_kwargs['modified_streams'] = set(modified_streams)

        # TODO: should we deprecate the following, and require R models to handle documents themselves?
        if modified_documents not in (_SENTINEL, ri.NULL):
            mod_docs = update_kwargs['modified_documents'] = {}

            for k, v in ListVector(modified_documents).items():
                if not isinstance(v, Vector) or len(v) != 1:
                    raise ValueError('Value for document "{}" must be a scalar.'.format(k))

                value = str(_extract_scalar(v))

                binding = port_bindings.get(k)
                if (binding is None) or ('documentId' not in binding):
                    mod_docs[k] = {'document': value}
                else:
                    document = analysis_client.get_document(binding['documentId'])
                    analysis_client.set_document_value(document, value=value)

        update(**update_kwargs)

    return ri.rternalize(wrapper)


def _convert_logger(logger):
    import rpy2.rinterface as ri

    def wrapper(message, level=None, file=None, line=None, timestamp=None):
        message = _extract_scalar(message)
        level = _extract_scalar(level)
        file = _extract_scalar(file)
        line = _extract_scalar(line)
        timestamp = _extract_scalar(timestamp)

        logger(message, level, file, line, timestamp)

    return ri.rternalize(wrapper)


def _extract_scalar(vector):
    return vector[0] if vector is not None and len(vector) == 1 else vector
