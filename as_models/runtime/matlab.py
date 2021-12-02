
import json
import os

from . import subprocess
from .runtime import ModelRuntime


class MatlabModelRuntime(ModelRuntime):
    REQUEST_FILE_PATH = '/tmp/job_request.json'

    def is_valid(self):
        # TODO: the "proper" way to do this would be to see if the entrypoint is on the classpath (perhaps using javap).
        try:
            _ = self._java_home
            return True
        except AttributeError:
            return False

    def execute_model(self, job_request, args, updater):
        # Dump the job request out to file - the Matlab code will read it in later.
        with open(MatlabModelRuntime.REQUEST_FILE_PATH, 'w') as f:
            json.dump(job_request, f)

        # Add job request and manifest paths to Matlab environment
        env = dict(os.environ, JOB_REQUEST_PATH=MatlabModelRuntime.REQUEST_FILE_PATH, MANIFEST_PATH=self.manifest_path)

        # Run the Matlab code using the Java runtime.
        updater.update()  # Marks the job as running.
        command = [self._jvm_path, '-cp', self._get_classpath(), self.entrypoint, MatlabModelRuntime.REQUEST_FILE_PATH,
                   self.manifest_path]
        self.logger.debug('Matlab execution environment: %s', env)
        self.logger.debug('Matlab execution command: %s', command)
        self.logger.info('NOTE: Output from Matlab is prefixed [MATLAB].')
        exit_code = subprocess.execute(command, updater, log_prefix='[MATLAB] ', env=env)

        if exit_code != 0:
            raise RuntimeError("Matlab model process failed with exit code {}.".format(exit_code))

    def _get_classpath(self):
        # Add a few useful entries to the classpath.
        classpath_entries = [
            '.',
            self.model_dir,
            *os.environ.get('CLASSPATH', '').split(os.pathsep),
            os.path.join(self._java_home, 'lib'),
            os.path.join(self._java_home, 'jre', 'lib')
        ]

        # Resolve absolute paths of classpath entries.
        classpath_entries = [MatlabModelRuntime._resolve_classpath_entry(entry) for entry in classpath_entries]

        # Remove duplicates and non-existing entries, preserving order.
        known = set()
        classpath_entries = [entry for entry in classpath_entries if entry and not (entry in known or known.add(entry))]

        return os.pathsep.join(classpath_entries)

    @staticmethod
    def _resolve_classpath_entry(entry):
        abs_entry = os.path.abspath(entry)
        if os.path.isfile(abs_entry):
            return abs_entry
        elif os.path.isdir(abs_entry):
            return os.path.join(abs_entry, '*')

    @property
    def _java_home(self):
        try:
            return os.environ['JAVA_HOME']
        except KeyError:
            raise AttributeError

    @property
    def _jvm_path(self):
        return os.path.join(self._java_home, 'bin', 'java')
