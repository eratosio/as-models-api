
import json
import os
import stat
from pathlib import Path

from . import subprocess
from .runtime import ModelRuntime


class MatlabModelRuntime(ModelRuntime):
    REQUEST_FILE_NAME = 'job_request.json'

    def is_valid(self):
        """Ensure that entrypoint is a valid file"""
        return os.path.isfile(self.entrypoint_path)

    def apply_entrypoint_permissions(self, entrypoint_file: Path):
        """Ensure that entrypoint file from model has execute permissions"""
        st = entrypoint_file.stat()

        # Ensure binary is executable and update permissions
        entrypoint_file.chmod(st.st_mode | stat.S_IXUSR)
        st = entrypoint_file.stat()
        self.logger.debug(f"Updated binary permissions: {stat.filemode(st.st_mode)} ({st.st_mode:o})")
       
        # Still worth keeping in case chmod command fails
        if not (st.st_mode & stat.S_IXUSR):
            raise RuntimeError(f"Matlab binary at {entrypoint_file} is not executable")

    def execute_model(self, job_request, args, updater):
        # Validate matlab binary
        matlab_binary = Path(self.entrypoint_path).resolve()
        if not matlab_binary.is_file():
            raise RuntimeError(f"Matlab binary not found at {matlab_binary}")
        
        self.apply_entrypoint_permissions(matlab_binary)

        # Dump the job request out to file - the Matlab code will read it in later.
        request_file_path = os.path.join(os.getcwd(), MatlabModelRuntime.REQUEST_FILE_NAME)
        with open(request_file_path, 'w') as f:
            json.dump(job_request, f)

        # Add job request and manifest paths to Matlab environment
        env = dict(os.environ, JOB_REQUEST_PATH=request_file_path, MANIFEST_PATH=self.manifest_path)

        # Run the Matlab code using the matlab runtime.
        updater.update()
        
        command = [str(matlab_binary), "-nodisplay", "-nosplash", "-nodesktop", "-batch"]

        self.logger.debug('Matlab execution environment: %s', env)
        self.logger.debug('Matlab execution command: %s', command)
        self.logger.info('NOTE: Output from Matlab is prefixed [MATLAB].')
        exit_code = subprocess.execute(command, updater, log_prefix='[MATLAB] ', env=env)

        if exit_code != 0:
            raise RuntimeError("Matlab model process failed with exit code {}.".format(exit_code))
