
import argparse

def host(args):
	from web_api import WebAPI
	
	WebAPI(args).run()

# Create main arg parser.
parser = argparse.ArgumentParser(description='Analysis Services Model Integration Engine')
subparsers = parser.add_subparsers()

# Create the parser for the "host" command
install_model_parser = subparsers.add_parser('host', help='Host a model')
install_model_parser.add_argument('entrypoint', help='The path to the main model file (i.e. its "entrypoint").')
install_model_parser.add_argument('-p', '--port', help='The port to run the web api on.', default=8080)
install_model_parser.add_argument('-t', '--type', help='The model type.', default=argparse.SUPPRESS)
install_model_parser.add_argument('-r', '--root', help='The model "root" directory.', default=argparse.SUPPRESS)
install_model_parser.add_argument('-d', '--debug', help='Run the model in debug mode?', action='store_true')
install_model_parser.set_defaults(func=host)

# TODO: install command?

# Parse command line.
namespace = parser.parse_args()
args = vars(namespace)

# Run selected function.
namespace.func(args)
