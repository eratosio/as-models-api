
import os
from setuptools import setup, find_packages

def read(filename):
	return open(os.path.join(os.path.dirname(__file__), filename)).read()

def get_version(version_file):
	ns = {}
	exec(read(version_file), ns)
	return ns['__version__']

setup(
	name = 'as_models',
    version = get_version('as_models/version.py'),
    author = 'Mac Coombe',
    author_email = 'mac.coombe@csiro.au',
    description = ('Analysis Services model integration library.'),
    keywords = 'models',
    # TODO: url = 'https://bitbucket.csiro.au/...',
    packages = find_packages(),
    long_description = read('readme.md'),
    install_requires = [
        'flask'
    ],
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Topic :: Scientific/Engineering :: Information Analysis',
        # TODO: 'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7'
    ]
)
