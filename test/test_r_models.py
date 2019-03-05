from rpy2.robjects import DataFrame
from tds_client import Dataset

from as_models.manifest import Model
from as_models.ports import INPUT_PORT, DOCUMENT_PORT, DOCUMENT_COLLECTION_PORT, STREAM_COLLECTION_PORT, \
    GRID_COLLECTION_PORT, STREAM_PORT, GRID_PORT

import unittest

from as_models.r_models import _convert_ports


class ContextTests(unittest.TestCase):
    def test_ports(self):
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
                'b': {'document': 'doc1'},
                'd': {'streamId': 'stream1'},
                'f': {'catalog': 'cat1.xml', 'dataset': 'data1.nc'}
            },
            'sensorCloudConfiguration': {
                "url": "https://52.64.57.4/api/sensor/v2",
                "apiKey": "714debe3bfc3464dc6364dbc5455f326"
            }
        }

        ports = _convert_ports(job_request.get('ports', {}))

        doc1 = ports.rx2('b')
        stream1 = ports.rx2('d')
        grid1 = ports.rx2('f')

        self.assertEqual('\"doc1\"', doc1.rx2('document').r_repr())
        self.assertEqual('\"stream1\"', stream1.rx2('streamId').r_repr())
        self.assertEqual('\"cat1.xml\"', grid1.rx2('catalog').r_repr())
        self.assertEqual('\"data1.nc\"', grid1.rx2('dataset').r_repr())



    def test_collection_ports(self):

        job_request = {
            'modelId': 'test',
            'ports': {
                'a': {'ports': [{'document': 'doc1'}, {'document': 'doc2'}]},
                'b': {'ports': [{'streamId': 'stream1'}, {'streamId': 'stream2'}]},
                'c': {'ports': [{'catalog': 'cat1.xml', 'dataset': 'data1.nc'}, {'catalog': 'cat2.xml', 'dataset': 'data2.nc'}]}
            },
            'sensorCloudConfiguration': {
                "url": "https://52.64.57.4/api/sensor/v2",
                "apiKey": "714debe3bfc3464dc6364dbc5455f326"
            }
        }

        ports = _convert_ports(job_request.get('ports', {}))

        doc1 = ports.rx2('a').rx2('ports')[0]
        doc2 = ports.rx2('a').rx2('ports')[1]

        self.assertEqual('\"doc1\"', doc1.rx2('document').r_repr())
        self.assertEqual(0L, long(doc1.rx2('index').r_repr()))
        self.assertEqual('\"doc2\"', str(doc2.rx2('document').r_repr()))
        self.assertEqual(1L, long(doc2.rx2('index').r_repr()))

        stream1 = ports.rx2('b').rx2('ports')[0]
        stream2 = ports.rx2('b').rx2('ports')[1]

        self.assertEqual('\"stream1\"', stream1.rx2('streamId').r_repr())
        self.assertEqual(0L, long(stream1.rx2('index').r_repr()))
        self.assertEqual('\"stream2\"', str(stream2.rx2('streamId').r_repr()))
        self.assertEqual(1L, long(stream2.rx2('index').r_repr()))

        grid1 = ports.rx2('c').rx2('ports')[0]
        grid2 = ports.rx2('c').rx2('ports')[1]

        self.assertEqual('\"cat1.xml\"', grid1.rx2('catalog').r_repr())
        self.assertEqual('\"data1.nc\"', grid1.rx2('dataset').r_repr())
        self.assertEqual(0L, long(grid1.rx2('index').r_repr()))
        self.assertEqual('\"cat2.xml\"', grid2.rx2('catalog').r_repr())
        self.assertEqual('\"data2.nc\"', grid2.rx2('dataset').r_repr())
        self.assertEqual(1L, long(grid2.rx2('index').r_repr()))