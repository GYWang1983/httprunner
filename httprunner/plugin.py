import time
import json
from httprunner import logger
from httprunner.response import ResponseObject
from httprunner.exceptions import ScriptExecuteError


class PluginResponse(ResponseObject):
    def __init__(self, data: dict, meta: dict):
        super().__init__(data)
        self.meta = meta

    def __getattr__(self, field: str):
        return self._extract_field_with_delimiter(field)

    def _extract_field_with_delimiter(self, field):
        try:
            top_query, sub_query = field.split('.', 1)
            if top_query == 'result':
                return self._extract_field(self.resp_obj, sub_query)
            elif top_query == 'metadata':
                return self._extract_field(self.meta, sub_query)
            else:
                return None
        except ValueError:
            if field == 'script':
                return self.meta['script']

        return None

    def _extract_field(self, data: dict, field: str):
        if not data:
            return None

        try:
            top_query, sub_query = field.split('.', 1)
            if top_query in data:
                return self._extract_field(data.get(top_query), sub_query)
        except ValueError:
            if field in data:
                return data.get(field)

        return None


class PluginBase:

    def __init__(self):

        self.meta_data = {
            "type": "plugin",
            "result": "",
            "stat": {
                "content_size": "N/A",
                "response_time_ms": "N/A",
                "elapsed_ms": "N/A",
            }
        }

        self.response = None

    def run(self, test_dict, name=""):

        self.meta_data['name'] = name
        self.meta_data['script'] = self.module
        self.meta_data['params'] = test_dict

        start = time.time()
        try:
            result = self.execute(test_dict)
            self.response = PluginResponse(result, self.meta_data)
            self.meta_data['result'] = json.dumps(result, indent=2)
            return self.response
        except Exception as e:
            error_str = str(e)
            logger.log_error(u"{exception}".format(exception=error_str))
            self.meta_data["error"] = error_str
            raise ScriptExecuteError(e)
        finally:
            self.meta_data['stat']['elapsed_ms'] = int((time.time() - start) * 1000)

    def response_time(self, rt):
        self.meta_data['stat']['response_time_ms'] = int(rt)
