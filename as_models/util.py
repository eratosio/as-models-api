
from senaps_sensor.auth import HTTPBasicAuth, HTTPKeyAuth

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

def resolve_service_config(url='', scheme=None, host=None, api_root=None, port=None, username=None, password=None, api_key=None, apiRoot=None, apiKey=None, verify=True):
    api_root = api_root or apiRoot
    api_key = api_key or apiKey
    
    # Resolve authentication.
    if api_key is not None:
        auth = HTTPKeyAuth(api_key, 'apikey')
    elif None not in (username, password):
        auth = HTTPBasicAuth(username, password)
    else:
        auth = None
    
    # Resolve API base URL and hostname.
    parts = urlparse.urlparse(url, scheme='http')
    scheme = parts[0] if scheme is None else scheme
    host = parts[1] if host is None else host
    api_root = parts[2] if api_root is None else api_root
    if port is not None:
        host = '{}:{}'.format(host.partition(':')[0], port)
    url = urlparse.urlunparse((scheme, host, api_root) + parts[3:])
    
    return url, host, api_root, auth, verify
