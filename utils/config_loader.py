import importlib.util

def load_config(path):
    spec = importlib.util.spec_from_file_location("cfg", path)
    cfg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cfg)
    return cfg
