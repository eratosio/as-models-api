
from as_models.util import urljoin, urlparse

import functools
import json
import posixpath
import re
import signal
import threading

try:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
except ImportError:
    from http.server import HTTPServer, BaseHTTPRequestHandler


class MockAnalysisServiceRequestHandler(BaseHTTPRequestHandler):
    def _handle_request(self):
        rpath = posixpath.relpath(self.path, self._api_base_path).strip(posixpath.sep)

        candidate_handlers = []
        for method, pattern, handler in MockAnalysisServiceRequestHandler.__method_lookup:
            if self.command != method:
                continue

            m = pattern.match(rpath)
            if m:
                score = len(m.group(0)) / len(rpath)
                candidate_handlers.append((score, handler, m.groupdict()))

        if not candidate_handlers:
            self._send_informational_response(500, 'API not mocked', 'The requested API endpoint is not mocked.')
            return

        _, handler, kwargs = sorted(candidate_handlers, reverse=True)[0]

        if self.command in ('PATCH', 'POST', 'PUT'):
            content_len = int(self.headers.get('content-length', 0))
            kwargs = dict(kwargs, body=self.rfile.read(content_len))

        handler(self, **kwargs)

    def _get_documents(self):
        self._send_json_response(200, {
            'count': len(self._documents),
            '_embedded': {
                'documentnodes': [
                    self._format_document(doc_id, doc) for doc_id, doc in self._documents.items()
                ]
            }
        })

    def _get_document(self, document_id):
        try:
            self._send_json_response(200, self._format_document(document_id, self._documents[document_id]))
        except KeyError:
            self._send_informational_response(404, 'Not found')

    def _get_document_value(self, document_id):
        try:
            self._send_response(200, 'application/octet-stream', self._documents[document_id]['value'])
        except KeyError:
            self._send_informational_response(404, 'Not found')

    def _put_document(self, document_id, body):
        self._documents[document_id] = json.loads(body.decode('utf-8'))
        self._get_document(document_id)

    def _delete_document(self, document_id):
        try:
            del self._documents[document_id]
            self._send_informational_response(201, 'Document deleted')
        except KeyError:
            self._send_informational_response(404, 'Not found')

    def _send_informational_response(self, status, message, developer_message=None):
        json_ = {
            'statuscode': status,
            'message': message
        }

        if developer_message is not None:
            json_['developermessage'] = developer_message

        self._send_json_response(status, json_)

    def _send_json_response(self, status, json_):
        self._send_response(status, 'application/json', json.dumps(json_))

    def _send_response(self, status, content_type, content):
        if isinstance(content, str):
            content = content.encode('utf-8')
            content_type += '; charset=UTF-8'

        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _format_document(self, document_id, document):
        result = document.copy()  # Prevent mutation of original.

        document_url = urljoin(self._api_base_url, 'documentnodes', document_id)

        result.pop('id', None)

        result['documentid'] = document_id
        result['_links'] = {'self': {'href': document_url}, 'value': {'href': urljoin(document_url, 'value')}}
        result['value'] = document['value'][:1024]
        result['valuetruncated'] = len(result['value']) < len(document['value'])

        return result

    @property
    def _api_base_url(self):
        return getattr(self.server, 'base_url')

    @property
    def _api_base_path(self):
        return getattr(self.server, 'base_path')

    @property
    def _documents(self):
        return getattr(self.server, 'documents')

    do_DELETE = do_GET = do_HEAD = do_OPTIONS = do_PATCH = do_POST = do_PUT = _handle_request

    __method_lookup = [
        ('GET', re.compile('documentnodes'), _get_documents),
        ('GET', re.compile('documentnodes/(?P<document_id>[^/]+)'), _get_document),
        ('GET', re.compile('documentnodes/(?P<document_id>[^/]+)/value'), _get_document_value),
        ('PUT', re.compile('documentnodes/(?P<document_id>[^/]+)'), _put_document),
        ('DELETE', re.compile('documentnodes/(?P<document_id>[^/]+)'), _delete_document)
    ]


class MockAnalysisServiceApi(HTTPServer):
    def __init__(self, base_path='/api/analysis', port=0):
        super(MockAnalysisServiceApi, self).__init__(('localhost', port), MockAnalysisServiceRequestHandler)

        self.__base_path = base_path
        self.__thread = None
        self.__documents = {}

    def start(self):
        if self.__thread is None:
            self.__thread = threading.Thread(target=self.serve_forever, daemon=True)
            self.__thread.start()

    def stop(self):
        if self.__thread is not None:
            self.shutdown()
            self.__thread.join()

    def set_document(self, id_, value, organisation_id, group_ids=None):
        self.documents[id_] = {
            'value': value,
            'organisationid': organisation_id,
            'groupids': [] if group_ids is None else group_ids
        }

    def activate(self, fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            self.start()
            result = fn(*args, **kwargs)
            self.stop()
            return result

        return wrapper

    def await_sigint(self):
        signal.signal(signal.SIGINT, lambda sig, frame: self.stop())
        print('Press Ctrl-C to stop.')
        signal.pause()

    @property
    def base_path(self):
        return self.__base_path

    @property
    def base_url(self):
        host, port = self.socket.getsockname()
        return urlparse.urlunparse(('http', '{}:{}'.format(host, port), self.base_path, '', '', ''))

    @property
    def documents(self):
        return self.__documents

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


if __name__ == '__main__':
    with MockAnalysisServiceApi() as api:
        print('Mock API available at ', api.base_url)
        api.await_sigint()

        print(api.documents)
