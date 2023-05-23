from contextlib import contextmanager
import importlib.util
import os
import sys


@contextmanager
def add_to_path(p):
    old_path = sys.path
    sys.path = sys.path[:]
    sys.path.insert(0, p)
    try:
        yield
    finally:
        sys.path = old_path


# import a module from a path.
# Solution from https://stackoverflow.com/questions/41861427/python-3-5-how-to-dynamically-import-a-module-given-the-full-file-path-in-the
def path_import(absolute_path):
    if not os.path.exists(absolute_path):
        raise Exception(f"Module path {absolute_path} not found!")
    with add_to_path(os.path.dirname(absolute_path)):
        spec = importlib.util.spec_from_file_location(absolute_path, absolute_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
