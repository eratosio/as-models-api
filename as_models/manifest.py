
# TODO: validation

import abc, json

class Entity(object):
    __metaclass__ = abc.ABCMeta
    
    def __eq__(self, other):
        return self.__key() == other.__key()
    
    def __hash__(self):
        return hash(self.__key())
    
    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, ', '.join(str(k) for k in self._key()))
    
    def __key(self):
        return (type(self),) + self._key()
    
    @abc.abstractmethod
    def _key(self):
        pass

class Port(Entity):
    def __init__(self, raw):
        self.name = raw['portName']
        self.direction = raw['direction']
        self.type = raw['type']
        self.description = raw.get('description', None)
        self.required = raw['required']
    
    def _key(self):
        return (self.name, self.direction, self.type, self.description, self.required)

class Model(Entity):
    def __init__(self, raw):
        self.id = raw['id']
        self.name = raw.get('name', None)
        self.version = raw.get('version', None)
        self.description = raw.get('description', None)
        self.method = raw.get('method', None)
        self.ports = frozenset(Port(port) for port in raw.get('ports', []))
    
    def _key(self):
        return (self.id, self.name, self.version, self.description, self.method, self.ports)

class Dependency(Entity):
    def __init__(self, raw):
        self.name = raw['name']
        self.provider = raw['provider']
        self.requires = frozenset(raw.get('requires', []))
    
    def _key(self):
        return (self.name, self.provider, self.requires)

class Manifest(Entity):
    def __init__(self, raw):
        self.base_image = raw['baseImage']
        self.organisation_id = raw['organisationId']
        self.group_ids = frozenset(raw.get('groupIds', []))
        self.entrypoint = raw['entrypoint']
        self.dependencies = frozenset(Dependency(dependency) for dependency in raw.get('dependencies', []))
        
        models = frozenset(Model(model) for model in raw['models'])
        self.models = { model.id:model for model in models }
    
    def _key(self):
        return (self.base_image, self.organisation_id, self.group_ids, self.entrypoint, self.dependencies, self.models)
    
    @staticmethod
    def from_file(path):
        with open(path) as f:
            return Manifest(json.load(f))
