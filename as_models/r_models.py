
from ports import DOCUMENT_PORT
from sentinel import SENTINEL

import os

# NOTE: this module makes frequent use of lazy imports to ensure rpy2 stuff is
# only imported on an as-needed basis.

def is_valid_entrypoint(entrypoint):
	entrypoint = os.path.abspath(entrypoint)
	
	return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.r')

def run_model(entrypoint, job_request, args, updater):
	from rpy2.robjects import r, conversion
	from rpy2.robjects.vectors import ListVector
	from rpy2.rinterface import NULL
	
	model_id = job_request['modelId']
	
	# Load the model's module.
	module = r.source(entrypoint)
	
	# Locate a function matching the model ID.
	try:
		implementation = r[model_id]
	except LookupError:
		raise RuntimeError('Unable to locate function "{}" in model "{}".'.format(model_id, entrypoint)) # TODO: more specific exception type?
	if not callable(implementation):
		raise RuntimeError('Member "{}" of model "{}" is not a callable function.'.format(model_id, entrypoint)) # TODO: more specific exception type?
	
	# Enable custom conversions.
	@conversion.py2ri.register(type(None))
	def convert_none(none):
		return NULL
	
	# Convert request to R-compatible.
	sensor_config = job_request.get('sensorCloudConfiguration', None)
	analysis_config = job_request.get('analysisServicesConfiguration', None)
	r_sensor_config = None if sensor_config is None else ListVector(sensor_config)
	r_analysis_config = None if analysis_config is None else ListVector(analysis_config)
	r_ports = _convert_ports(job_request.get('ports', {}))
	r_update = _convert_update(updater.update)
	r_logger = _convert_logger(updater.log)
	
	# Run the implementation.
	implementation(r_ports, r_sensor_config, r_analysis_config, r_update, r_logger)

def _convert_ports(ports):
	from rpy2.robjects.vectors import ListVector
	
	return ListVector((k, ListVector(dict({}, name=k, direction=v.pop('direction').lower(), **v))) for k,v in ports.iteritems())

def _convert_update(update):
	import rpy2.rinterface as ri
	from rpy2.robjects.vectors import Vector, ListVector
	
	def wrapper(message=SENTINEL, progress=SENTINEL, modified_streams=SENTINEL, modified_documents=SENTINEL):
		update_kwargs = {}
		
		if message not in (SENTINEL, ri.NULL):
			update_kwargs['message'] = _extract_scalar(message)
		if progress not in (SENTINEL, ri.NULL):
			update_kwargs['progress'] = _extract_scalar(progress)
		if modified_streams not in (SENTINEL, ri.NULL):
			update_kwargs['modified_streams'] = set(modified_streams)
		if modified_documents not in (SENTINEL, ri.NULL):
			mod_docs = update_kwargs['modified_documents'] = {}
			for k,v in ListVector(modified_documents).items():
				if not isinstance(v, Vector) or len(v) != 1:
					raise ValueError('Value for document "{}" must be a scalar.'.format(k))
				mod_docs[k] = str(_extract_scalar(v))
		
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
