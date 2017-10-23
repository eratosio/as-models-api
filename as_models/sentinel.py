
class Sentinel(object):
    def __init__(self, repr='Sentinel'):
        self._repr = repr
    
    def __repr__(self):
        return self._repr
    
    def __eq__(self, other):
        return self is other
    
    def or_default(self, value, default):
        return default if value is self else value
