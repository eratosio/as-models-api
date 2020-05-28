import os
from .ports import DOCUMENT_PORT, OUTPUT_PORT
from .sentinel import Sentinel

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
    import rpy2.rinterface as ri

    model_id = job_request['modelId']
    model = manifest.models[model_id]
    # Load the model's module.
    module = r.source(entrypoint)

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

    # Convert request to R-compatible.
    r_sensor_config = _convert_service_config(job_request.get('sensorCloudConfiguration', None))
    r_analysis_config = _convert_service_config(job_request.get('analysisServicesConfiguration', None))
    r_thredds_config = _convert_service_config(job_request.get('threddsConfiguration', None))
    r_ports = _convert_ports(model.ports, job_request.get('ports', {}))
    r_update = _convert_update(updater.update, model, job_request.get('ports', {}))
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


def _convert_ports(model_ports, port_bindings):
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

            result[str(port_name)] = list(map(lambda i: ListVector({str(k): v for k, v in i.items()}), inner_ports))
        else:
            result[str(port_name)] = ListVector(dict(**{str(k): v for k, v in port_config.items()}))

    return ListVector(result)


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


def _convert_update(update, model, port_bindings):
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
        if modified_documents not in (_SENTINEL, ri.NULL):
            mod_docs = update_kwargs['modified_documents'] = {}
            for k, v in ListVector(modified_documents).items():
                if not isinstance(v, Vector) or len(v) != 1:
                    raise ValueError('Value for document "{}" must be a scalar.'.format(k))
                mod_docs[k] = {'document': str(_extract_scalar(v))}

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
