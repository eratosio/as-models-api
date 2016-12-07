
import os

def is_valid_entrypoint(entrypoint):
	entrypoint = os.path.abspath(entrypoint)
	
	return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.r')

def run_model(entrypoint, context):
	# Load the model's module.
	from rpy2.robjects import r # Lazy import
	module = r.source(entrypoint)
	
	# Locate a function matching the model ID.
	try:
		implementation = r[context.model_id]
	except LookupError:
		raise RuntimeError('Unable to locate function "{}" in model "{}".'.format(context.model_id, entrypoint)) # TODO: more specific exception type?
	if not callable(implementation):
		raise RuntimeError('Member "{}" of model "{}" is not a callable function.'.format(context.model_id, entrypoint)) # TODO: more specific exception type?
	
	# TODO: convert context to R-compatible.
	
	# Run the implementation.
	return implementation()
