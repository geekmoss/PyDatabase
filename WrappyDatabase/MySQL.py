class MySQL:
    """
    Wrapper class over PyMySQL

    :param str host:
    :param str user:
    :param str passwd:
    :param str db:
    """
    _conf = {}
    _conn = None
    _cur = None

    def __init__(self, host='localhost', user='root', passwd='toor', db='my_db', port=3306):
        """
        :param str host:
        :param str user:
        :param str passwd:
        :param str db:
        """
        self._conf = {
            'host': host,
            'user': user,
            'password': passwd,
            'db': db,
            'port': port,
            'charset': 'utf8mb4',
            'cursorclass': cursors.DictCursor
        }
        self.open()
        pass

    def open(self):
        """
        Opens the connection
        :return:
        """
        self._conn = connect(**self._conf)
        self._cur = self._conn.cursor()
        pass

    def close(self):
        """
        Closes the connection
        :return:
        """
        self._conn.close()
        pass

    def query(self, query, params=None, commit=False):
        """
        Sends the query to the database
        :param str query:
        :param tuple|dict|None params:
        :param bool commit:
        :return MySQLWrapperCursor:
        """
        cursor = MySQLCursor(self._cur, query, params)
        if commit:
            self.commit()
            pass
        return cursor

    def commit(self):
        """
        Commit the transaction
        :return:
        """
        self._conn.commit()
        pass

    def rollback(self):
        """
        Rollback the transaction
        :return:
        """
        self._conn.rollback()
        pass

    def mogrify(self, query, params):
        return self._conn.cursor().mogrify(query, params)

    def inserted_id(self):
        return self._conn.insert_id()
    pass


class MySQLCursor:
    _cur = None
    _sql = None
    _params = None

    def __init__(self, cur, sql, params):
        """
        :param pymysql.cursors.DictCursor cur:
        :param str sql:
        :param tuple|dict|None params:
        """
        self._cur = cur
        self._sql = sql
        self._params = params
        self._cur.execute(sql, params)
        pass

    def fetch(self):
        """
        Returns the next row or None
        :return:
        """
        return self._cur.fetchone()

    def fetch_many(self, size=None):
        """
        Returns the specified number of rows
        :param size:
        :return:
        """
        return self._cur.fetchmany(size)

    def fetch_all(self):
        """
        Returns the all rows
        :return:
        """
        return self._cur.fetchall()

    def get_query(self):
        """
        Returns the specified query
        :return:
        """
        return self._sql

    def get_params(self):
        """
        Returns the specified params
        :return:
        """
        return self._params

    def row_count(self):
        """
        Returns the number of affected rows
        :return:
        """
        return self._cur.rowcount

    def builded_query(self):
        """
        Returns builded query
        :return:
        """
        return self._cur.mogrify(self._sql, self._params)

    def __iter__(self):
        return self

    def __next__(self):
        res = self.fetch()
        if res is not None:
            return res
        else:
            raise StopIteration()
        pass
    pass


class MySQLException(Exception):
    pass


try:
    from pymysql import cursors, connect
except ImportError:
    MySQLException('Failed to import the PyMySQL module. Please make sure it is installed.')
    pass

