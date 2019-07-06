# -*- coding: UTF-8 -*-

import datetime
from as_models.exceptions import SenapsModelError


def noop(context):
    pass


def required_ports_model_in1_out1(context):
    pass


def test_model(context):
    raise SenapsModelError('BOOM', user_data={
        'senap_model_err_name': 'an_error',
        'err_datetime': datetime.datetime.now()})
    # pass some user data using our new exception class. Also a field that is illegal.
    pass


def summarise(port, type):
    if port:
        actions = {
            'document': lambda port: port.value or '',
            'document_collection': lambda ports: [actions['document'](x) for x in ports],
            'stream': lambda port: port.stream_id or '',
            'stream_collection': lambda ports: [actions['stream'](x) for x in ports],
            'grid': lambda port: (port.catalog_url, port.dataset_path) if port.catalog_url else '',
            'grid_collection': lambda ports: [actions['grid'](x) for x in ports],
        }

        print('%s: %s' % (port.name, actions[type](port)))


def all_port_types_model(context):
    # summarise(context.ports['input_document'], 'document')
    # summarise(context.ports['output_document'], 'document')
    # summarise(context.ports['input_documents'], 'document_collection')
    # summarise(context.ports['output_documents'], 'document_collection')
    #
    # summarise(context.ports['input_stream'], 'stream')
    # summarise(context.ports['output_stream'], 'stream')
    # summarise(context.ports['input_streams'], 'stream_collection')
    # summarise(context.ports['output_streams'], 'stream_collection')
    #
    # summarise(context.ports['input_grid'], 'grid')
    # summarise(context.ports['output_grid'], 'grid')
    # summarise(context.ports['input_grids'], 'grid_collection')
    # summarise(context.ports['output_grids'], 'grid_collection')

    for (idx, doc) in enumerate(context.ports['input_documents']):
        print('processing input_documents[%d] = %s' % (idx, doc.value))
        new_value = '%s %d' % (doc.value, idx)
        print('writing output_documents[%d].value = %s' % (idx, new_value))
        context.ports['output_documents'][idx].value = new_value

    context.ports['output_document'].value = context.ports['input_document'].value + ' updated'


def test_error(context):
    raise SenapsModelError("something went wrong", {"foo": "bar"})
