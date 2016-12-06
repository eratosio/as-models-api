
import os

def is_valid_entrypoint(entrypoint):
	entrypoint = os.path.abspath(entrypoint)
	
	return os.path.isfile(entrypoint) and (os.path.splitext(entrypoint)[1].lower() == '.r')

def run_model(entrypoint, context):
	pass # TODO
