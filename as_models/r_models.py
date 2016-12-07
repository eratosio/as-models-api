
from sentinel import SENTINEL

import os

# NOTE: this module makes frequent use of lazy imports to ensure rpy2 stuff is
# only imported on an as-needed basis.

def is_valid_entrypoint(entrypoint):
	entrypoint = os.path.abspath(entrypoint)
	
	return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.r')

def run_model(entrypoint, context, args, job_request):
	from rpy2.robjects import r, conversion
	from rpy2.robjects.vectors import ListVector
	from rpy2.rinterface import NULL
	
	# Load the model's module.
	module = r.source(entrypoint)
	
	# Locate a function matching the model ID.
	try:
		implementation = r[context.model_id]
	except LookupError:
		raise RuntimeError('Unable to locate function "{}" in model "{}".'.format(context.model_id, entrypoint)) # TODO: more specific exception type?
	if not callable(implementation):
		raise RuntimeError('Member "{}" of model "{}" is not a callable function.'.format(context.model_id, entrypoint)) # TODO: more specific exception type?
	
	# Create a custom converter.
	@conversion.py2ri.register(type(None))
	def convert_none(none):
		return NULL
	
	# Convert context to R-compatible.
	sensor_config = job_request.get('sensorCloudConfiguration', None)
	analysis_config = job_request.get('analysisServicesConfiguration', None)
	r_sensor_config = None if sensor_config is None else ListVector(sensor_config)
	r_analysis_config = None if analysis_config is None else ListVector(analysis_config)
	r_ports = _convert_ports(job_request.get('ports', {}))
	r_update = _convert_update(context.update)
	
	# Run the implementation.
	implementation(r_ports, r_sensor_config, r_analysis_config, r_update)

def _convert_ports(ports):
	from rpy2.robjects.vectors import ListVector
	
	return ListVector((k, ListVector(dict(v, name=k))) for k,v in ports.iteritems())

def _convert_update(update):
	import rpy2.rinterface as ri
	
	def wrapper(message=SENTINEL, progress=SENTINEL):
		update_kwargs = {}
		
		if message is not SENTINEL:
			update_kwargs['message'] = None if message is None else message[0]
		if progress is not SENTINEL:
			update_kwargs['progress'] = None if progress is None else progress[0]
		
		update(**update_kwargs)
	
	return ri.rternalize(wrapper)
