import importlib, types
from httprunner.plugin import PluginBase


imported_module = importlib.import_module("scripts.tcpping")
for name, item in vars(imported_module).items():
    if type(item) == type:
        clz = types.new_class(name, (item,), kwds={"metaclass": PluginBase})
        print(clz)
        print(clz())



