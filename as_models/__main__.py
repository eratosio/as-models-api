
import argparse

def run(args):
	if 'port' in args:
		args['mode'] = 'web'
	
	mode = args.get('mode', 'local').lower()
	if mode == 'web':
		pass # TODO: run web front-end
	elif mode == 'local':
		pass # TODO: run model locally
	else:
		print 'Unsupported run mode "{}".'.format(mode)

# Create main arg parser.
parser = argparse.ArgumentParser(description='Analysis Services Model Integration Engine')
subparsers = parser.add_subparsers()

# Create the parser for the "run" command
install_model_parser = subparsers.add_parser('run', help='Run a model', parents=[opts_parser])
install_model_parser.add_argument('entrypoint', help='The path to the main model file (i.e. its "entrypoint").')
install_model_parser.add_argument('-m', '--mode', help='The "run-mode", either "web" or "local".')
install_model_parser.add_argument('-p', '--port', help='The port to run the web api on. Implies "--mode web".', default=argpare.SUPPRESS)
install_model_parser.add_argument('-t', '--type', help='The model type.')
install_model_parser.set_defaults(func=run)

# TODO: install command

# Parse command line.
namespace = parser.parse_args()
args = vars(namespace)

# Run selected function.
namespace.func(args)
