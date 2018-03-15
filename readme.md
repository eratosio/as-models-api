
# Unit testing:

Unit tests may be run with the following command:

    python -m unittest discover

# Coverage Reporting:

Coverage reporting is handled by the Python [coverage](https://coverage.readthedocs.io)
library. If necessary, it can be installed as follows:

    sudo pip install coverage

An HTML coverage report can then be generated with these commands:

    coverage run -m unittest discover
    coverage html

The results will be placed in the '/htmlcov' directory.
   
