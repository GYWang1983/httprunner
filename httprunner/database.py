from sqlalchemy import create_engine
from sqlalchemy import exc
from httprunner import exceptions, logger, utils
from httprunner.response import ResponseObject

sqlalchemy_dialect_mapping = {
    'mysql': 'mysql+pymysql'
}

class SqlRunner(object):
    """ Execute sql.
        Examples:
        >>> query = {
            dialect: mysql
            connection: root@localhost/testrunner;
            sql: SELECT status FROM user WHERE uid=$uid;
        }
    """

    def __init__(self):
        self.init_meta_data()

    def init_meta_data(self):
        """ initialize meta_data, it will store detail data of query result
        """
        self.meta_data = {
            "type": "database",
            "name": "",
            "data": {
                "connection": {
                    "dialect": "",
                    "url": "",
                    "user": "",
                },
                "sql": "",
                "result": {

                }
            },
            "stat": {
                "content_size": "N/A",
                "response_time_ms": "N/A",
                "elapsed_ms": "N/A",
            }
        }

    def execute(self, query, name=None):

        self.init_meta_data()

        dialect = query.get('dialect')
        if not dialect:
            dialect = 'mysql'

        conn_config = query.get('connection')
        url = conn_config.get('url')
        user = conn_config.get("user")
        passwd = conn_config.get("password")

        if passwd:
            auth = "{}:{}".format(user, passwd)
        else:
            auth = user

        conn_str = "{}://{}@{}".format(sqlalchemy_dialect_mapping.get(dialect.lower()), auth, url)
        sql = query.get('sql')

        # record test name
        self.meta_data["name"] = name

        # record original query info
        self.meta_data["data"]["connection"]["dialect"] = dialect
        self.meta_data["data"]["connection"]["url"] = url
        self.meta_data["data"]["connection"]["user"] = user
        self.meta_data["sql"] = sql

        conn = None
        try:
            logger.log_debug("connect to database: {}".format(conn_str))
            engine = create_engine(conn_str)
            conn = engine.connect()
            result = conn.execute(sql)
            return DatabaseResult(result)

        except exc.DatabaseError as e:
            error_str = str(e)
            logger.log_error(u"{exception}".format(exception=error_str))
            self.meta_data["error"] = error_str
            raise exceptions.DatabaseQueryError(e)
        finally:
            if conn:
                conn.close()


# engine = create_engine('mysql+pymysql://root@localhost/testrunner', echo=True)
# conn = engine.connect()
# result = conn.execute("select * from `case`")
# n = result.rowcount
# print(n)
# for row in result:
#     print("row:", row.items())

class DatabaseResult(ResponseObject):

    def __init__(self, result):
        super().__init__(result)
        self.resp_obj = result
        self.count = result.rowcount

    def __getattr__(self, key):
        try:
            if key == "json":
                value = self.resp_obj.json()
            elif key == "cookies":
                value = self.resp_obj.cookies.get_dict()
            else:
                value = getattr(self.resp_obj, key)

            self.__dict__[key] = value
            return value
        except AttributeError:
            err_msg = "ResponseObject does not have attribute: {}".format(key)
            logger.log_error(err_msg)
            raise exceptions.ParamsError(err_msg)

    def _extract_field_with_delimiter(self, field):
        """ database result content could be sqlalchemy.engine.ResultProxy.

        Args:
            field (str): string joined by delimiter.
            e.g.
                "status_code"
                "headers"
                "cookies"
                "content"
                "headers.content-type"
                "content.person.name.first_name"

        """
        # string.split(sep=None, maxsplit=-1) -> list of strings
        # e.g. "content.person.name" => ["content", "person.name"]
        try:
            top_query, sub_query = field.split('.', 1)
        except ValueError:
            top_query = field
            sub_query = None

        # status_code
        if top_query in ["count"]:
            if sub_query:
                # status_code.XX
                err_msg = u"Failed to extract '{}' from database result\n".format(field)
                logger.log_error(err_msg)
                raise exceptions.ParamsError(err_msg)

            return getattr(self, top_query)

        # cookies
        elif top_query == "cookies":
            cookies = self.cookies
            if not sub_query:
                # extract cookies
                return cookies

            try:
                return cookies[sub_query]
            except KeyError:
                err_msg = u"Failed to extract cookie! => {}\n".format(field)
                err_msg += u"response cookies: {}\n".format(cookies)
                # CHANGED BY gy.wang: extract failure not cause case failure
                # logger.log_error(err_msg)
                # raise exceptions.ExtractFailure(err_msg)
                logger.log_warning(err_msg)
                return None

        # elapsed
        elif top_query == "elapsed":
            available_attributes = u"available attributes: days, seconds, microseconds, total_seconds"
            if not sub_query:
                err_msg = u"elapsed is datetime.timedelta instance, attribute should also be specified!\n"
                err_msg += available_attributes
                logger.log_error(err_msg)
                raise exceptions.ParamsError(err_msg)
            elif sub_query in ["days", "seconds", "microseconds"]:
                return getattr(self.elapsed, sub_query)
            elif sub_query == "total_seconds":
                return self.elapsed.total_seconds()
            else:
                err_msg = "{} is not valid datetime.timedelta attribute.\n".format(sub_query)
                err_msg += available_attributes
                logger.log_error(err_msg)
                raise exceptions.ParamsError(err_msg)

        # headers
        elif top_query == "headers":
            headers = self.headers
            if not sub_query:
                # extract headers
                return headers

            try:
                return headers[sub_query]
            except KeyError:
                err_msg = u"Failed to extract header! => {}\n".format(field)
                err_msg += u"response headers: {}\n".format(headers)
                # CHANGED BY gy.wang: extract failure not cause case failure
                # logger.log_error(err_msg)
                # raise exceptions.ExtractFailure(err_msg)
                logger.log_warning(err_msg)
                return None

        # response body
        elif top_query in ["content", "text", "json"]:
            try:
                body = self.json
            except exceptions.JSONDecodeError:
                body = self.text

            if not sub_query:
                # extract response body
                return body

            if isinstance(body, (dict, list)):
                # content = {"xxx": 123}, content.xxx
                return utils.query_json(body, sub_query)
            elif sub_query.isdigit():
                # content = "abcdefg", content.3 => d
                return utils.query_json(body, sub_query)
            else:
                # content = "<html>abcdefg</html>", content.xxx
                err_msg = u"Failed to extract attribute from response body! => {}\n".format(field)
                err_msg += u"response body: {}\n".format(body)
                # CHANGED BY gy.wang: extract failure not cause case failure
                # logger.log_error(err_msg)
                # raise exceptions.ExtractFailure(err_msg)
                logger.log_warning(err_msg)
                return None

        # new set response attributes in teardown_hooks
        elif top_query in self.__dict__:
            attributes = self.__dict__[top_query]

            if not sub_query:
                # extract response attributes
                return attributes

            if isinstance(attributes, (dict, list)):
                # attributes = {"xxx": 123}, content.xxx
                return utils.query_json(attributes, sub_query)
            elif sub_query.isdigit():
                # attributes = "abcdefg", attributes.3 => d
                return utils.query_json(attributes, sub_query)
            else:
                # content = "attributes.new_attribute_not_exist"
                err_msg = u"Failed to extract cumstom set attribute from teardown hooks! => {}\n".format(field)
                err_msg += u"response set attributes: {}\n".format(attributes)
                logger.log_error(err_msg)
                raise exceptions.TeardownHooksFailure(err_msg)

        # others
        else:
            err_msg = u"Failed to extract attribute from response! => {}\n".format(field)
            err_msg += u"available response attributes: status_code, cookies, elapsed, headers, content, text, json, encoding, ok, reason, url.\n\n"
            err_msg += u"If you want to set attribute in teardown_hooks, take the following example as reference:\n"
            err_msg += u"response.new_attribute = 'new_attribute_value'\n"
            logger.log_error(err_msg)
            raise exceptions.ParamsError(err_msg)

    def log_error_message(self, query_data):
        pass
