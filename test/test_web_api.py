import functools
import json
import time

from requests import HTTPError

from as_models.manifest import Manifest
from as_models.web_api import _load_entrypoint

import requests

import os, multiprocessing, sys, unittest

from as_models.web_api import app

# Necessary to import rpy2 globally here - lazy loading dependencies in r_models.run_model on MACOS causes underlying crash
from rpy2.robjects import r, conversion
from rpy2.rinterface import NULL
from rpy2.robjects.vectors import ListVector

def get_model_path(lang='python'):
    if lang == 'python':
        test_model_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_model')
        test_model_manifest_path = os.path.join(test_model_dir, 'manifest.json')
        test_model_entrypoint_path = os.path.join(test_model_dir, 'model.py')
        test_model_manifest = Manifest.from_file(os.path.join(test_model_dir, 'manifest.json'))
    else:
        test_model_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'r_test_model')
        test_model_manifest_path = os.path.join(test_model_dir, 'manifest.json')
        test_model_entrypoint_path = os.path.join(test_model_dir, 'model.R')
        test_model_manifest = Manifest.from_file(os.path.join(test_model_dir, 'manifest.json'))

    return {
        'model_path': test_model_dir,
        'manifest_path': test_model_manifest_path,
        'entrypoint_path': test_model_entrypoint_path,
        'manifest': test_model_manifest
    }

def host_model(model_path, port):
    # sys.stdout = open('{}.out'.format(port), 'w')
    # sys.stderr = open('{}.err'.format(port), 'w')
    
    app.config['model_path'] = model_path

    return app.test_client()

class TestModelClient(object):
    def __init__(self, port, test_client):
        self._test_client = test_client
    
    def start(self, payload=None): # TODO: model params

        p = payload or {
            'modelId': 'test_model',
            'ports': {
                'input': { 'document': 'test_document' },
                'output': { 'document': 'placeholder' }
            }
        }

        return self._test_client.post(json=p).json

    def poll(self):
        return self._test_client.get().json

    def terminate(self, timeout):
        return self._test_client.post("terminate", json={'timeout': timeout}).json


class TestModel(object):
    def __init__(self, port, lang='python'):
        self._port = port
        self._proc = None
        self._model_path = get_model_path(lang)['model_path']

    def __enter__(self):
        self._test_client = host_model(self._model_path, self._port)
        self._test_client.__enter__()
        return TestModelClient(self._port, self._test_client)
    
    def __exit__(self, *args):
        self._test_client.__exit__(None, None, None)

class EntrypointTests(unittest.TestCase):
    def test_load_from_directory(self):
        resources = get_model_path()

        manifest, entrypoint = _load_entrypoint(resources['model_path'])
        
        self.assertEqual(manifest, resources['manifest'])
        self.assertEqual(entrypoint, resources['entrypoint_path'])
    
    def test_load_from_manifest(self):
        resources = get_model_path()

        manifest, entrypoint = _load_entrypoint(resources['manifest_path'])
        
        self.assertEqual(manifest, resources['manifest'])
        self.assertEqual(entrypoint, resources['entrypoint_path'])
    
    def test_load_from_entrypoint(self):
        resources = get_model_path()

        manifest, entrypoint = _load_entrypoint(resources['entrypoint_path'])
        
        self.assertEqual(manifest, resources['manifest'])
        self.assertEqual(entrypoint, resources['entrypoint_path'])


class HostTests(unittest.TestCase):
    def test_hosting_test_model(self):

        for lang in ['r', 'python']:
            self.run_model(lang)

    def run_model(self, lang='python', payload_json=None, callback=None):

        print('--- Executing Test (lang=%s) ---'%lang)

        with TestModel(8000, lang) as model:
            # Send job start request.
            response = model.start(payload_json)
            self.handle_response(callback, response)

            if 'state' not in response:
                raise KeyError("state not found in response: " + str(response))

            # Poll for model completion.
            while response['state'] not in ('COMPLETE', 'FAILED'):
                response = model.poll()
                self.handle_response(callback, response)

            # Allow up to 10 seconds for the model to terminate.
            response = model.terminate(10.0)

            self.handle_response(callback, response)

            # self.assertEqual('Model shut down cleanly.', terminate_response.get('log', [])[-1]['message'])

            exception_details = response.get('exception', '')
            print(exception_details)

            # we'd like to know the fields in exception are set correctly.
            if len(exception_details) > 0:
                self.assertTrue('developer_msg' in exception_details)
                self.assertTrue('data' in exception_details)
                self.assertTrue('model_id' in exception_details)

        return response

    def handle_response(self, callback, response):
        self.print_logs(response)
        if callback:
            callback(response)

    def print_logs(self, response):
        if 'log' in response and len(response['log']) > 0:
            print(json.dumps(response['log'], indent=4, sort_keys=True))

    def test_all_port_types_model_r(self):
            response = self.run_model('r', {
                    "modelId": "all_port_types_model",
                    "ports": {
                        "input_documents": { "ports": [{ "documentId": "indoc1", "document": "foo" }, { "document": "bar" }] },
                        "input_streams": { "ports": [{"streamId": "s1"}, {"streamId": "s2"}]},
                        "output_documents": {"ports": [{ "documentId": "outdoc1", "document": "foo foo"}, { "documentId": "outdoc2", "document": "bar bar"}]},
                        "input_document": {"document": "single input"},
                        "output_document": {"documentId": "abc", "document": "single output"}
                    }
                })

            results = response['results']

            # TODO: test collection updates when supported...
            output_document = results['output_document']

            self.assertNotIn('input_documents', results)
            self.assertNotIn('input_document', results)
            self.assertEqual('single input updated', output_document['document'])

    def test_all_port_types_model_python(self):
        response = self.run_model('python', {
            "modelId": "all_port_types_model",
            "ports": {
                "input_documents": {"ports": [{"documentId": "indoc1", "document": "foo"}, {"document": "bar"}]},
                "input_streams": {"ports": [{"streamId": "s1"}, {"streamId": "s2"}]},
                "output_documents": {"ports": [{"documentId": "outdoc1", "document": "foo foo"},
                                               {"documentId": "outdoc2", "document": "bar bar"}]},
                "input_document": {"document": "single input"},
                "output_document": {"documentId": "abc", "document": "single output"}
            }
        })

        results = response['results']

        output_document = results['output_document']
        output_documents = results['output_documents']['ports']

        print(results)
        self.assertNotIn('input_documents', results)
        self.assertNotIn('input_document', results)
        self.assertEqual('single input updated', output_document['document'])
        self.assertEqual('foo 0', output_documents[0]['document'])
        self.assertEqual('bar 1', output_documents[1]['document'])

    def test_missing_required_ports_should_warn_not_fail(self):

        for lang in ['r', 'python']:

            self._all_logs = []

            def callback(res):
                self._all_logs = self._all_logs + res.get('log', [])

            self.run_model(lang, {
                'modelId': 'required_ports_model_in1_out1',
                'ports': {
                    'in1': { 'value': 'assigned value ok' }
                    # out1 is the missing port and should warn but not fail...
                }
            }, callback)

            first_log_message = self._all_logs[0]['message']

            for term in ['Missing', 'required', 'port', 'out1']:
                self.assertIn(term, first_log_message)

            for term in ['in1']: # in1 is fine, shouldn't be reported on...
                self.assertNotIn(term, first_log_message)

            self.assertEqual('WARNING', self._all_logs[0]['level'])

    def test_log_flushes_per_request(self):
        for lang in ['r', 'python']:
            self._all_logs = []

            def callback(res):
                # confirm each log entry is new
                for log in res.get('log', []):
                    self.assertTrue(log not in self._all_logs, json.dumps(log) + " found in " + json.dumps(self._all_logs))
                # combine logs
                self._all_logs = self._all_logs + res.get('log', [])

            self.run_model(lang, {
                'modelId': 'all_port_types_model',
                'ports': {}
            }, callback)

    def test_errors_are_caught_and_reported(self):
        for lang in ['r', 'python']:
            self._all_logs = []

            def callback(res):
                # combine logs
                self._all_logs = self._all_logs + res.get('log', [])

            response = self.run_model(lang, {
                'modelId': 'test_error',
                'ports': {}
            }, callback)

            self.assertIsNotNone(response.get('exception', None))
            self.assertTrue('something went wrong' in response['exception']['developer_msg'])
            self.assertTrue(self._all_logs[0]['level'] in ['CRITICAL', 'STDERR'],
                            "expecting log error level of 'CRITICAL' or 'STDERR'")


if __name__ == "__main__":
    unittest.main()
