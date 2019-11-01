from sqlalchemy import create_engine
from httprunner import exceptions, logger
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
        passwd = "123"

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

        try:
            logger.log_debug("connect to database: {}".format(conn_str))
            engine = create_engine(conn_str)
            conn = engine.connect()

            result = conn.execute(sql)
            print(result.rowcount)

        except Exception as e:
            logger.log_error(u"{exception}".format(exception=str(e)))
            raise



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