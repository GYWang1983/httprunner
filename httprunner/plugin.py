import time
from httprunner import logger
from httprunner.response import ResponseObject
from httprunner.exceptions import ScriptExecuteError


class PluginResponse(ResponseObject):
    def __init__(self):
        super().__init__({})


class PluginBase:

    def __init__(self):

        self.meta_data = {
            "type": "plugin",
            "data": {},
            "stat": {
                "content_size": "N/A",
                "response_time_ms": "N/A",
                "elapsed_ms": "N/A",
            }
        }

        self.response = PluginResponse()

    def run(self, test_dict, name=""):

        self.meta_data['name'] = name
        self.meta_data['script'] = self.module
        self.meta_data['params'] = test_dict

        start = time.time()
        try:
            self.execute(test_dict)
        except Exception as e:
            error_str = str(e)
            logger.log_error(u"{exception}".format(exception=error_str))
            self.meta_data["error"] = error_str
            raise ScriptExecuteError(e)
        finally:
            self.meta_data['stat']['elapsed_ms'] = int((time.time() - start) * 1000)

    def response_time(self, rt):
        self.meta_data['stat']['response_time_ms'] = int(rt)
