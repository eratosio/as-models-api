
from as_models.manifest import Model
from as_models.ports import INPUT_PORT, DOCUMENT_PORT
from as_models.python_models import Context

import unittest

class ContextTests(unittest.TestCase):
    def test_ports(self):
        port_b_document = 'port_b_document'
        default_document = 'default_document'
        
        model = Model({
            'id': 'test',
            'ports': [
                { 'portName': 'a', 'type': DOCUMENT_PORT, 'direction': INPUT_PORT, 'required': False },
                { 'portName': 'b', 'type': DOCUMENT_PORT, 'direction': INPUT_PORT, 'required': False }
            ]
        })
        job_request = {
            'modelId': 'test',
            'ports': {
                'b': { 'document': port_b_document }
            },
            'sensorCloudConfiguration': {
                "url": "https://52.64.57.4/api/sensor/v2",
                "apiKey": "714debe3bfc3464dc6364dbc5455f326"
            }
        }
        context = Context(model, job_request, {}, None)
        
        port_a = context.ports['a']
        self.assertFalse(port_a.was_supplied)
        self.assertEqual(default_document, port_a.get(default_document))
        
        port_b = context.ports['b']
        self.assertTrue(port_b.was_supplied)
        self.assertEqual(port_b_document, port_b.get(default_document))

        self.assertEqual(context.sensor_client.connect_retries, 10)
        self.assertEqual(context.sensor_client.status_retries, 10)
        self.assertEqual(context.sensor_client.read_retries, 10)
        self.assertEqual(context.sensor_client.timeout, 300)
