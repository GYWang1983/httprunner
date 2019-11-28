from httprunner.response import ResponseObject


class PluginResponse(ResponseObject):
    def __init__(self):
        super().__init__({})


class PluginBase:

    def __init__(self):

        self.meta_data = {
            "type": "plugin",
            "stat": {
                "content_size": "N/A",
                "response_time_ms": "N/A",
                "elapsed_ms": "N/A",
            }
        }

        self.response = PluginResponse()



