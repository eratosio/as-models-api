import json

from requests import HTTPError

from as_models.manifest import Manifest
from as_models.web_api import _load_entrypoint

import requests

import os, multiprocessing, sys, unittest

from as_models.web_api import app

test_model_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_model')
test_model_manifest_path = os.path.join(test_model_dir, 'manifest.json')
test_model_entrypoint_path = os.path.join(test_model_dir, 'model.py')
test_model_manifest = Manifest.from_file(os.path.join(test_model_dir, 'manifest.json'))

def host_model(model_path, port):
    sys.stdout = open('{}.out'.format(port), 'w')
    sys.stderr = open('{}.err'.format(port), 'w')
    
    app.config['model_path'] = model_path
    app.run(host='0.0.0.0', port=port)

class TestModelClient(object):
    def __init__(self, port):
        self._base_url = 'http://localhost:{}/'.format(port)
    
    def start(self, payload=None): # TODO: model params
        input_document = 'test_document'
        p = payload or {
            'modelId': 'test_model',
            'ports': {
                'input': { 'document': input_document },
                'output': { 'document': 'placeholder' }
            }
        }
        
        return self._request('POST', json=p)

    def poll(self):
        return self._request('GET')
    
    def terminate(self, timeout):
        return self._request('POST', path='terminate', json={'timeout': timeout})
    
    def _request(self, method, path='', *args, **kwargs):
        url = self._base_url + path
        
        response = requests.request(method, url, *args, **kwargs)

        try:
            response.raise_for_status()
        except HTTPError as e:
            raise RuntimeError(e.response.json())
        
        return response.json()

class TestModel(object):
    def __init__(self, port):
        self._port = port
        self._proc = None
    
    def __enter__(self):
        self._proc = multiprocessing.Process(target=host_model, args=(test_model_manifest_path, self._port))
        self._proc.start()
        
        return TestModelClient(self._port)
    
    def __exit__(self, *args):
        self._proc.terminate()
        self._proc.join(10)

class EntrypointTests(unittest.TestCase):
    def test_load_from_directory(self):
        manifest, entrypoint = _load_entrypoint(test_model_dir)
        
        self.assertEqual(manifest, test_model_manifest)
        self.assertEqual(entrypoint, test_model_entrypoint_path)
    
    def test_load_from_manifest(self):
        manifest, entrypoint = _load_entrypoint(test_model_manifest_path)
        
        self.assertEqual(manifest, test_model_manifest)
        self.assertEqual(entrypoint, test_model_entrypoint_path)
    
    def test_load_from_entrypoint(self):
        manifest, entrypoint = _load_entrypoint(test_model_entrypoint_path)
        
        self.assertEqual(manifest, test_model_manifest)
        self.assertEqual(entrypoint, test_model_entrypoint_path)
        
class HostTests(unittest.TestCase):
    def test_hosting_test_model(self):
        with TestModel(8000) as model:
            # Send job start request.
            response = model.start()
            self.assertEqual('PENDING', response['state'])
            
            # Poll for model completion.
            while response['state'] not in ('COMPLETE', 'FAILED'):
                response = model.poll()
                print(json.dumps(response['log'], indent=4, sort_keys=True))
                print(response.get('exception', ''))

            self.assertEqual('COMPLETE', response['state'])
            
            # Allow up to 10 seconds for the model to terminate.
            response = model.terminate(10.0)
            self.assertEqual('Model shut down cleanly.', response['log'][-1]['message'])

    def test_collection_ports(self):
        with TestModel(8000) as model:
            # Send job start request.
            response = model.start({
                "modelId": "all_port_types_model",
                "ports": {
                    "input_documents": { "ports": [{ "document": "foo" }, { "document": "bar" }] }
                }
            })

            self.assertEqual('PENDING', response['state'])

            # Poll for model completion.
            while response['state'] not in ('COMPLETE', 'FAILED'):
                response = model.poll()
                print(json.dumps(response['log'], indent=4, sort_keys=True))
                print(response.get('exception', ''))

            self.assertEqual('COMPLETE', response['state'])

            # Allow up to 10 seconds for the model to terminate.
            response = model.terminate(10.0)
            self.assertEqual('Model shut down cleanly.', response['log'][-1]['message'])

    def test_missing_required_ports_should_warn_not_fail(self):
        with TestModel(8000) as model:
            # Send job start request.
            response = model.start({
                'modelId': 'required_ports_model_in1_out1',
                'ports': {
                    'in1': { 'value': 'assigned value ok' }
                    # out1 is the missing port and should warn but not fail...
                }
            })

            self.assertEqual('PENDING', response['state'])

            # Poll for model completion.
            while response['state'] not in ('COMPLETE', 'FAILED'):
                response = model.poll()
                print(json.dumps(response['log'], indent=4, sort_keys=True))
                print(response.get('exception', ''))

            self.assertEqual('COMPLETE', response['state'])

            # Allow up to 10 seconds for the model to terminate.
            response = model.terminate(10.0)

            first_log_message = response['log'][0]['message']

            for term in ['Missing', 'required', 'port', 'out1']:
                self.assertIn(term, first_log_message)

            for term in ['in1']: # in1 is fine, shouldn't be reported on...
                self.assertNotIn(term, first_log_message)

            self.assertEqual('WARNING', response['log'][0]['level'])
