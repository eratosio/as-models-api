
_models = {}

def model(alias):
    def wrapper(model):
        global _models
        _models[alias] = model
        return model
    
    return wrapper
