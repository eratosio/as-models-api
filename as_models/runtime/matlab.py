
import json
import os

from . import subprocess
from .runtime import ModelRuntime
from .. import log_levels


class MatlabModelRuntime(ModelRuntime):
    REQUEST_FILE_PATH = '/tmp/job_request.json'

    def is_valid(self):
        # TODO: the "proper" way to do this would be to see if the entrypoint is on the classpath (perhaps using javap).
        try:
            _ = self._java_home
        except AttributeError:
            return False

    def execute_model(self, job_request, args, updater):
        # Dump the job request out to file - the Matlab code will read it in later.
        with open(MatlabModelRuntime.REQUEST_FILE_PATH, 'w') as f:
            json.dump(job_request, f)

        # Run the Matlab code using the Java runtime.
        updater.update()  # Marks the job as running.
        exit_code = subprocess.execute([self._jvm_path, '-cp', self._get_classpath(), self.entrypoint,
                                        MatlabModelRuntime.REQUEST_FILE_PATH, self.manifest_path], updater)

        if exit_code != 0:
            updater.log("Matlab model process failed with exit code {}.".format(exit_code), level=log_levels.CRITICAL)

    def _get_classpath(self):
        classpath_entries = [
            os.path.join('.', '*'),
            os.path.join(self.model_dir, '*'),
            *os.environ.get('CLASSPATH', '').split(os.pathsep),
            os.path.join(self._java_home, 'lib', '*'),
            os.path.join(self._java_home, 'jre', 'lib', '*')
        ]

        classpath_entries = [os.path.abspath(entry) for entry in classpath_entries]

        # Remove duplicates and non-existing entries, preserving order.
        known = set()
        classpath_entries = [
            entry for entry in classpath_entries if os.path.exists(entry) and not (entry in known or known.add(entry))
        ]

        return os.pathsep.join(classpath_entries)

    @property
    def _java_home(self):
        try:
            return os.environ['JAVA_HOME']
        except KeyError:
            raise AttributeError

    @property
    def _jvm_path(self):
        return os.path.join(self._java_home, 'bin', 'java')
