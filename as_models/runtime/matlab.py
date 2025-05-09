
import json
import os

from . import subprocess
from .runtime import ModelRuntime


class MatlabModelRuntime(ModelRuntime):
    REQUEST_FILE_NAME = 'job_request.json'

    def is_valid(self):
        """Ensure that .jar files are rejected on model upload"""
        return os.path.isfile(self.entrypoint_path) and os.path.splitext(self.entrypoint_path)[1].lower() != '.jar'

    def execute_model(self, job_request, args, updater):
        # Dump the job request out to file - the Matlab code will read it in later.
        request_file_path = os.path.join(os.getcwd(), MatlabModelRuntime.REQUEST_FILE_NAME)
        with open(request_file_path, 'w') as f:
            json.dump(job_request, f)

        # Add job request and manifest paths to Matlab environment
        env = dict(os.environ, JOB_REQUEST_PATH=request_file_path, MANIFEST_PATH=self.manifest_path)

        # Run the Matlab code using the matlab runtime.
        updater.update()  # Marks the job as running.
        command = [self.entrypoint]
        self.logger.debug('Matlab execution environment: %s', env)
        self.logger.debug('Matlab execution command: %s', command)
        self.logger.info('NOTE: Output from Matlab is prefixed [MATLAB].')
        exit_code = subprocess.execute(command, updater, log_prefix='[MATLAB] ', env=env)

        if exit_code != 0:
            raise RuntimeError("Matlab model process failed with exit code {}.".format(exit_code))
