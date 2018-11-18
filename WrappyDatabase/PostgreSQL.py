import psycopg2
import psycopg2.extras
from psycopg2._psycopg import connection, cursor


class PostgreSQL:
    """
    Wrapper class over PostreSQL
    """
    _conf = {}
    _conn: connection = None
    _cur: cursor = None

    def __init__(self, host='localhost', user='root', passwd='toor', db='my_db', port=5432):
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
            'database': db,
            'port': port,
        }

        self.open()
        pass

    def open(self):
        """
        Opens the connection
        :return:
        """
        self._conn: connection = psycopg2.connect(**self._conf)
        self._conn.set_client_encoding('UTF8')
        self._cur: cursor = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        pass

    def close(self):
        """
        Closes the connection
        :return:
        """
        self._conn.close()
        pass

    def query(self, query, params=(), commit=False):
        """
        Sends the query to the database
        :param str query:
        :param tuple or dict or None params:
        :param bool commit:
        :return PostreSQLWrapperCursor:
        """
        cur = PostgreSQLCursor(self._cur, query, params)
        if commit:
            self.commit()
            pass
        return cur

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

    def mogrify(self, query, params) -> str:
        return self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor).mogrify(query, params)
    pass


class PostgreSQLCursor:
    _cur = None
    _sql = None
    _params = None

    def __init__(self, cur: cursor, sql, params):
        self._cur = cur
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

    def __iter__(self):
        return self

    def __next__(self):
        res = self.fetch()
        if res is not None:
            return dict(res)
        else:
            raise StopIteration()
        pass
    pass


class PostgreSQLException(Exception):
    pass
