import unittest

from as_models import testing
from as_models.ports import STREAM_PORT, INPUT_PORT, STREAM_COLLECTION_PORT, DOCUMENT_COLLECTION_PORT, DOCUMENT_PORT, \
    GRID_PORT, GRID_COLLECTION_PORT
from as_models.testing.mock import MockAnalysisServiceApi


class TestTesting(unittest.TestCase):
    mock_as = MockAnalysisServiceApi()

    def setUp(self):
        self.context = testing.Context()

    def test_configure_ports(self):
        self.context.configure_port("stream", STREAM_PORT, INPUT_PORT, stream_id="s1")
        self.context.configure_port("stream[]", STREAM_COLLECTION_PORT, INPUT_PORT, stream_ids=["s1", "s2"])

        self.context.configure_port("doc", DOCUMENT_PORT, INPUT_PORT, value="abc")
        self.context.configure_port("doc_empty_str", DOCUMENT_PORT, INPUT_PORT, value="")
        self.context.configure_port("doc_none", DOCUMENT_PORT, INPUT_PORT, value=None)
        self.context.configure_port("doc[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, values=["v1", "v2"])
        self.context.configure_port("doc_empty_str[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, values=["", "v2"])
        self.context.configure_port("doc_empty_list[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, values=[])

        self.context.configure_port("docId", DOCUMENT_PORT, INPUT_PORT, document_id="d1")
        self.context.configure_port("docId[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, document_ids=["d1", "d2"])

        self.context.configure_port("grid", GRID_PORT, INPUT_PORT, catalog_url="cat", dataset_path="dat")
        self.context.configure_port("grid[]", GRID_COLLECTION_PORT, INPUT_PORT, catalog_urls=["cat1", "cat2"], dataset_paths=["dat1", "dat2"])

        self.assertEqual("s1", self.context.ports["stream"].stream_id)
        self.assertEqual("s1", self.context.ports["stream[]"][0].stream_id)
        self.assertEqual("s2", self.context.ports["stream[]"][1].stream_id)

        self.assertEqual("abc", self.context.ports["doc"].value)
        self.assertEqual(True, self.context.ports["doc"].was_supplied)
        self.assertEqual("", self.context.ports["doc_empty_str"].value)
        self.assertEqual(True, self.context.ports["doc_empty_str"].was_supplied)
        self.assertEqual(None, self.context.ports["doc_none"].value)
        self.assertEqual(False, self.context.ports["doc_none"].was_supplied)
        self.assertEqual("v1", self.context.ports["doc[]"][0].value)
        self.assertEqual("v2", self.context.ports["doc[]"][1].value)
        self.assertEqual("", self.context.ports["doc_empty_str[]"][0].value)
        self.assertEqual("v2", self.context.ports["doc_empty_str[]"][1].value)
        self.assertEqual(0, len(self.context.ports["doc_empty_list[]"]))
        self.assertEqual(True, self.context.ports["doc_empty_list[]"].was_supplied)

        self.assertEqual("d1", self.context.ports["docId"].document_id)
        self.assertEqual("d1", self.context.ports["docId[]"][0].document_id)
        self.assertEqual("d2", self.context.ports["docId[]"][1].document_id)

        self.assertEqual("cat", self.context.ports["grid"].catalog_url)
        self.assertEqual("dat", self.context.ports["grid"].dataset_path)
        self.assertEqual("cat1", self.context.ports["grid[]"][0].catalog_url)
        self.assertEqual("dat1", self.context.ports["grid[]"][0].dataset_path)
        self.assertEqual("cat2", self.context.ports["grid[]"][1].catalog_url)
        self.assertEqual("dat2", self.context.ports["grid[]"][1].dataset_path)

    @mock_as.activate
    def test_configure_ports_with_mocked_as(self):
        self.context.configure_analysis_client(url=TestTesting.mock_as.base_url)

        self.context.configure_port("doc", DOCUMENT_PORT, INPUT_PORT, value="abc")

        self.context.configure_port("doc_org_grp", DOCUMENT_PORT, INPUT_PORT, value="abc2", doc_organisation_id='test_org', doc_group_ids=['g1'])
        self.context.configure_port("doc_org", DOCUMENT_PORT, INPUT_PORT, value="abc3", doc_organisation_id='test_org')

        self.assertEqual("abc2", self.context.analysis_client.get_document_value(self.context.ports.get('doc_org_grp').document_id))
        self.assertEqual("abc3", self.context.analysis_client.get_document_value(self.context.ports.get('doc_org').document_id))

        self.context.configure_port("doc_empty_str", DOCUMENT_PORT, INPUT_PORT, value="")
        self.context.configure_port("doc_none", DOCUMENT_PORT, INPUT_PORT, value=None)
        self.context.configure_port("doc[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, values=["v1", "v2"])
        self.context.configure_port("doc_empty_str[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, values=["", "v2"])
        self.context.configure_port("doc_empty_list[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, values=[])

        TestTesting.mock_as.set_document('d1', 'd1 value', 'test_org')
        TestTesting.mock_as.set_document('d2', 'd2 value', 'test_org')

        self.context.configure_port("docId", DOCUMENT_PORT, INPUT_PORT, document_id="d1")
        self.context.configure_port("docId[]", DOCUMENT_COLLECTION_PORT, INPUT_PORT, document_ids=["d1", "d2"])

        self.assertEqual("abc", self.context.ports["doc"].value)
        self.assertEqual(True, self.context.ports["doc"].was_supplied)
        self.assertEqual("", self.context.ports["doc_empty_str"].value)
        self.assertEqual(True, self.context.ports["doc_empty_str"].was_supplied)
        self.assertEqual(None, self.context.ports["doc_none"].value)
        self.assertEqual(False, self.context.ports["doc_none"].was_supplied)
        self.assertEqual("v1", self.context.ports["doc[]"][0].value)
        self.assertEqual("v2", self.context.ports["doc[]"][1].value)
        self.assertEqual("", self.context.ports["doc_empty_str[]"][0].value)
        self.assertEqual("v2", self.context.ports["doc_empty_str[]"][1].value)
        self.assertEqual(0, len(self.context.ports["doc_empty_list[]"]))
        self.assertEqual(True, self.context.ports["doc_empty_list[]"].was_supplied)

        self.assertEqual("d1", self.context.ports["docId"].document_id)
        self.assertEqual("d1", self.context.ports["docId[]"][0].document_id)
        self.assertEqual("d2", self.context.ports["docId[]"][1].document_id)

        self.assertEqual("d1 value", self.context.ports["docId"].value)
        self.assertEqual("d1 value", self.context.ports["docId[]"][0].value)
        self.assertEqual("d2 value", self.context.ports["docId[]"][1].value)


