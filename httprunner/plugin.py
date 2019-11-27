from httprunner.response import ResponseObject


class PluginResponse(ResponseObject):
    def __init__(self):
        super().__init__({})


class PluginBase(type):
    meta_data = {
        "type": "plugin",
        "stat": {
            "content_size": "N/A",
            "response_time_ms": "N/A",
            "elapsed_ms": "N/A",
        }
    }

    response = PluginResponse()


