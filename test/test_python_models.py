from tds_client import Dataset

from as_models.manifest import Model
from as_models.ports import INPUT_PORT, DOCUMENT_PORT, DOCUMENT_COLLECTION_PORT, STREAM_COLLECTION_PORT, \
    GRID_COLLECTION_PORT, STREAM_PORT, GRID_PORT
from as_models.python_models import Context

import unittest

class ContextTests(unittest.TestCase):
    def test_ports(self):
        port_b_document = 'port_b_document'
        default_document = 'default_document'

        model = Model({
            'id': 'test',
            'ports': [
                {'portName': 'a', 'type': DOCUMENT_PORT, 'direction': INPUT_PORT, 'required': False},
                {'portName': 'b', 'type': DOCUMENT_PORT, 'direction': INPUT_PORT, 'required': False},
                {'portName': 'c', 'type': STREAM_PORT, 'direction': INPUT_PORT, 'required': False},
                {'portName': 'd', 'type': STREAM_PORT, 'direction': INPUT_PORT, 'required': False},
                {'portName': 'e', 'type': GRID_PORT, 'direction': INPUT_PORT, 'required': False},
                {'portName': 'f', 'type': GRID_PORT, 'direction': INPUT_PORT, 'required': False}
            ]
        })
        job_request = {
            'modelId': 'test',
            'ports': {
                'b': {'document': port_b_document},
                'd': {'streamId': 'stream1'},
                'f': {'catalog': 'https://senaps.io/catalog.xml', 'dataset': 'dataset.nc'}
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

        port_c = context.ports['c']
        self.assertFalse(port_c.was_supplied)
        self.assertEqual(None, port_c.get(None))

        port_d = context.ports['d']
        self.assertTrue(port_d.was_supplied)
        self.assertEqual('stream1', port_d.get(None))

        port_e = context.ports['e']
        self.assertFalse(port_e.was_supplied)
        self.assertEqual(None, port_e.get(None))

        port_f = context.ports['f']
        self.assertTrue(port_f.was_supplied)
        self.assertEqual('https://senaps.io/catalog.xml', port_f.catalog_url)
        self.assertEqual('dataset.nc', port_f.dataset_path)
        # self.assertIsInstance(Dataset, port_f.get(None))

        self.assertEqual(context.sensor_client.connect_retries, 10)
        self.assertEqual(context.sensor_client.status_retries, 10)
        self.assertEqual(context.sensor_client.read_retries, 10)
        self.assertEqual(context.sensor_client.timeout, 300)

    def test_collection_ports(self):
        port_a_document = 'port_a_document'
        port_b_document = 'port_b_document'

        model = Model({
            'id': 'test',
            'ports': [
                {'portName': 'a', 'type': DOCUMENT_COLLECTION_PORT, 'direction': INPUT_PORT, 'required': False},
                {'portName': 'b', 'type': STREAM_COLLECTION_PORT, 'direction': INPUT_PORT, 'required': False},
                {'portName': 'c', 'type': GRID_COLLECTION_PORT, 'direction': INPUT_PORT, 'required': False},
            ]
        })
        job_request = {
            'modelId': 'test',
            'ports': {
                'a': {'ports': [{'document': port_a_document}, {'document': port_b_document}]},
                'b': {'ports': [{'streamId': 'stream1'}, {'streamId': 'stream2'}]},
                'c': {'ports': [{'catalog': 'cat1.xml', 'dataset': 'data1.nc'}, {'catalog': 'cat2.xml', 'dataset': 'data2.nc'}]}
            },
            'sensorCloudConfiguration': {
                "url": "https://52.64.57.4/api/sensor/v2",
                "apiKey": "714debe3bfc3464dc6364dbc5455f326"
            }
        }
        context = Context(model, job_request, {}, None)

        port_a = context.ports['a']
        self.assertTrue(port_a.was_supplied)
        self.assertEqual(2, len(port_a.ports))
        self.assertEqual(port_a_document, port_a.ports[0].value)
        self.assertEqual(port_b_document, port_a.ports[1].value)

        port_b = context.ports['b']
        self.assertTrue(port_b.was_supplied)
        self.assertEqual(2, len(port_b.get(None)))
        self.assertEqual('stream1', port_b.ports[0].stream_id)
        self.assertEqual('stream2', port_b.ports[1].stream_id)

        port_c = context.ports['c']
        self.assertTrue(port_c.was_supplied)
        self.assertEqual(2, len(port_c.get(None)))
        self.assertEqual('cat1.xml', port_c.ports[0].catalog_url)
        self.assertEqual('data1.nc', port_c.ports[0].dataset_path)
        self.assertEqual('cat2.xml', port_c.ports[1].catalog_url)
        self.assertEqual('data2.nc', port_c.ports[1].dataset_path)

        self.assertEqual(context.sensor_client.connect_retries, 10)
        self.assertEqual(context.sensor_client.status_retries, 10)
        self.assertEqual(context.sensor_client.read_retries, 10)
        self.assertEqual(context.sensor_client.timeout, 300)
