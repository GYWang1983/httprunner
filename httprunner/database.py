import time
import sqlparse
from sqlalchemy import create_engine
from sqlalchemy import exc
from unittest import SkipTest
from httprunner import exceptions, logger
from httprunner.response import ResponseObject

sqlalchemy_dialect_mapping = {
    'mysql': 'mysql+pymysql',
    'oracle': 'oracle+cx_oracle',
    'db2': 'ibm_db_sa',
    'mssql': 'mssql+pyodbc',
    'postgresql': 'postgresql+psycopg2'
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
                "result": None
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

        sql_array = []
        statements = sqlparse.parse(query.get('sql'))
        for stmt in statements:
            sql_type = stmt.get_type()
            sql = sqlparse.format(stmt.value, strip_comments=True, strip_whitespace=True)
            if sql:
                sql_array.append((sql_type, sql))

        if not sql_array:
            raise SkipTest("SQL is empty")

        # record original query info
        self.meta_data["name"] = name
        self.meta_data["data"]["sql"] = sql_array
        self.meta_data["data"]["connection"]["dialect"] = dialect
        self.meta_data["data"]["connection"]["url"] = url
        self.meta_data["data"]["connection"]["user"] = user

        conn = None
        resp = None
        start_connect_timestamp = time.time()

        try:
            url_parts = url.split('://')
            if len(url_parts) >= 2:
                url = url_parts[1]

            conn_str = "{}://{}@{}".format(sqlalchemy_dialect_mapping.get(dialect.lower()), auth, url)
            logger.log_debug("connect to database: {}".format(conn_str))

            engine = create_engine(conn_str)
            conn = engine.connect()

            start_exec_timestamp = time.time()
            for sql_type, sql in sql_array:
                if sql_type != 'UNKNOWN':
                    result = conn.execute(sql)
                    resp = DatabaseResult(self.meta_data, result, sql_type)
                else:
                    # Try to run
                    try:
                        conn.execute(sql)
                    except:
                        pass

            self.meta_data['stat']['response_time_ms'] = round((time.time() - start_exec_timestamp) * 1000, 2)

            self.meta_data["data"]["result"] = resp
            return resp

        except exc.DatabaseError as e:
            error_str = str(e)
            logger.log_error(u"{exception}".format(exception=error_str))
            self.meta_data["error"] = error_str
            raise exceptions.DatabaseQueryError(e)
        finally:
            if conn:
                conn.close()
                self.meta_data['stat']['elapsed_ms'] = round((time.time() - start_connect_timestamp) * 1000, 2)


class DatabaseResult(ResponseObject):

    def __init__(self, meta, result, type):
        super().__init__(result)
        self.meta_data = meta
        self.type = type
        self.resp_obj = result
        self.count = result.rowcount
        if type == 'SELECT':
            self.rows = result.fetchall()

    def __getattr__(self, key):
        if key in ('meta_data', 'type'):
            return getattr(key)
        else:
            return self._extract_field_with_delimiter(key)
        # try:
        #     value = getattr(self.resp_obj, key)
        #     return value
        # except AttributeError:
        #     err_msg = "ResultObject does not have attribute: {}".format(key)
        #     logger.log_error(err_msg)
        #     raise exceptions.ParamsError(err_msg)

    def _top_records(self, n=0):
        rs = []
        i = 0
        for row in self.rows:
            if i > n:
                break
            rs.append(dict(row))
            i += 1
        return rs

    def _record(self, row=0):
        return dict(self.rows[row]) if row < self.count else {}

    def _value(self, row=0, col='0'):
        if row >= self.count:
            return None
        else:
            try:
                return self.rows[row][int(col)] if col.isdigit() else self.rows[row][col]
            except exc.NoSuchColumnError | IndexError:
                logger.log_warning("No column {} in result set.\n".format(col))
                return None

    def _extract_field_with_delimiter(self, field):
        """ database result content could be sqlalchemy.engine.ResultProxy.

        Args:
            field (str): string joined by delimiter.
            e.g.
                "count"
                "result"
                "result.0"
                "result.0.col"
                "first"
                "first.col"
                "top" (equals to first)
                "top.1"
        """
        if self.type != 'SELECT':
            return None

        path = field.split('.')

        # count
        if path[0] == "count":
            return self.count

        # TODO: gy.wang: elapsed
        # elif top_query == "elapsed":
        #     available_attributes = u"available attributes: days, seconds, microseconds, total_seconds"
        #     if not sub_query:
        #         err_msg = u"elapsed is datetime.timedelta instance, attribute should also be specified!\n"
        #         err_msg += available_attributes
        #         logger.log_error(err_msg)
        #         raise exceptions.ParamsError(err_msg)
        #     elif sub_query in ["days", "seconds", "microseconds"]:
        #         return getattr(self.elapsed, sub_query)
        #     elif sub_query == "total_seconds":
        #         return self.elapsed.total_seconds()
        #     else:
        #         err_msg = "{} is not valid datetime.timedelta attribute.\n".format(sub_query)
        #         err_msg += available_attributes
        #         logger.log_error(err_msg)
        #         raise exceptions.ParamsError(err_msg)

        # result set
        elif path[0] == 'result':
            if len(path) == 1:
                # 'result' -> all result in dict
                return self._top_records()
            elif path[1].isdigit():
                if len(path) == 2:
                    # 'result.{num}' -> one row in dict
                    return self._record(int(path[1]))
                else:
                    # 'result.{num}.{col}' -> a column value
                    return self._value(int(path[1]), path[2])
            else:
                err_msg = u"Failed to extract attribute from result set! => {}\n".format(field)
                logger.log_warning(err_msg)
                return None

        elif path[0] == 'top':
            if len(path) == 1:
                # 'top' -> first row in dict
                return self._record(0)
            elif path[1].isdigit():
                # 'top.{num}' -> top n row in dict
                return self._top_records(int(path[1]))
            else:
                err_msg = u"Failed to extract attribute from result set! => {}\n".format(field)
                logger.log_warning(err_msg)
                return None

        elif path[0] == 'first':
            if len(path) == 1:
                # 'first' -> first row in dict
                return self._record(0)
            elif len(path) == 2:
                # 'first.{col}' -> column value in first row
                return self._value(0, path[1])
            else:
                err_msg = u"Failed to extract attribute from result set! => {}\n".format(field)
                logger.log_warning(err_msg)
                return None
        # others
        else:
            err_msg = u"Failed to extract attribute from database result! => {}\n".format(field)
            err_msg += u"available attributes: count, result, top, first.\n\n"
            logger.log_error(err_msg)
            raise exceptions.ParamsError(err_msg)

    def is_select(self):
        return self.type == 'SELECT'

    def keys(self):
        return self.resp_obj.keys() if self.is_select() else []

    def log_error_message(self, query_data):
        err_msg = "{} DETAILED REQUEST & RESPONSE {}\n".format("*" * 32, "*" * 32)

        # log connection
        connection = self.meta_data['data']['connection']
        if connection:
            err_msg += "====== connection details ======\n"
            err_msg += "dialect: {}\n".format(connection['dialect'])
            err_msg += "url: {}\n".format(connection['url'])
            err_msg += "user: {}\n".format(connection['user'])
            err_msg += "\n"

        # log error
        if 'error' in self.meta_data:
            err_msg += "====== errors ======\n"
            err_msg += self.meta_data['error']
            err_msg += "\n"
        logger.log_error(err_msg)
