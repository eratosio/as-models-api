
# Unit testing:

Unit tests may be run with the following command:

    python -m unittest discover

## Unit testing with Docker

With this method, you can test using the same environment that as-models-api natively runs in.

By default, we provide a `docker-compose.yml` file that will run the unittests on both Python 2.7 and Python 3.5

To run the tests on the working copy of your source:

    docker-compose up

The first run will take a while to pull down and build the images. Subsequent runs should be very fast.

# Coverage Reporting:

Coverage reporting is handled by the Python [coverage](https://coverage.readthedocs.io)
library. If necessary, it can be installed as follows:

    sudo pip install coverage

An HTML coverage report can then be generated with these commands:

    coverage run -m unittest discover
    coverage html

The results will be placed in the '/htmlcov' directory.
