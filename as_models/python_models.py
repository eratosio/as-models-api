
import importlib, os, sys

def is_valid_entrypoint(entrypoint):
	entrypoint = os.path.abspath(entrypoint)
	
	return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.py')

def run_model(entrypoint, context):
	# Load the model's module.
	model_dir, model_file = os.path.split(entrypoint)
	module_name, module_ext = os.path.splitext(model_file)
	sys.path.append(model_dir)
	module = importlib.import_module(module_name)
	
	# Locate a callable matching the model ID.
	implementation = getattr(module, context.model_id, None)
	if not callable(implementation):
		raise RuntimeError('Unable to locate callable "{}" in model "{}".'.format(context.model_id, entrypoint)) # TODO: more specific exception type?
	
	# Run the implementation.
	return implementation(context)
