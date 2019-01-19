from typing import Union
from .PostgreSQL import PostgreSQL
from .MySQL import MySQL
from .SqlBuilder import SqlBuilder


class Database:
    MYSQL = 1
    POSTGRESQL = 2
    _ports = {MYSQL: 3306, POSTGRESQL: 5432}
    """

    DSN: scheme://user:pass@host:port/database
    DSN for PostgreSQL: postgresql://root:toor@localhost/my_db
    DSN for MySQL: mysql://root:toor@localhost/my_db
    
    For driver use class const
    """
    def __init__(self, dsn=None, host=None, usr=None, passwd=None, db=None, port: int = None, driver: int = None):
        """
        :param dsn:
        :param host:
        :param usr:
        :param passwd:
        :param db:
        """
        self._driver: MySQL or PostgreSQL = None

        if dsn is None and (host is None or usr is None or passwd is None or db is None or driver is None):
            raise DatabaseException('Use DSN or arguments.')

        if dsn is not None:
            driver, args = self._dsn_parse(dsn)
            self._host = args['host']
            self._db = args['db']
            if driver == self.MYSQL:
                self._driver = MySQL(**args)
                pass
            elif driver == self.POSTGRESQL:
                self._driver = PostgreSQL(**args)
                pass
            pass
        else:
            if driver is not None:
                self._host = host
                self._db = db
                if driver == self.MYSQL:
                    if port is None:
                        port = self._ports[driver]
                    self._driver = MySQL(host=host, user=usr, passwd=passwd, db=db, port=None)
                    pass
                elif driver == self.POSTGRESQL:
                    if port is None:
                        port = self._ports[driver]
                    self._driver = PostgreSQL(host=host, user=usr, passwd=passwd, port=None, db=db)
                    pass
                else:
                    raise DatabaseException('An incorrect driver was selected. Please specify using a class constant.')
                pass
            else:
                raise DatabaseException("The driver is not defined. Please specify using a class constant.")
            pass
        pass
    
    def __repr__(self):
        return f"<Driver: {type(self._driver).__name__}, Host: {self._host}, DB: {self._db}>"

    def _dsn_parse(self, dsn: str):
        driver, dsn = dsn.split('://', 1)
        driver: str = driver.lower()
        if driver not in ['mysql', 'postgresql', 'postgres']:
            raise DatabaseException('The DSN contains an unsupported scheme.')

        if driver in ['postgres', 'postgresql']:
            driver = self.POSTGRESQL
            pass
        elif driver == 'mysql':
            driver = self.MYSQL
            pass

        login, path = dsn.split('@', 1)
        if ':' in login:
            user, passwd = login.split(':', 1)
        else:
            user = login
            passwd = ''

        host, db = path.split('/', 1)
        if ':' in host:
            host, port = host.split(':', 1)
            port = int(port)
            pass
        else:
            port = self._ports[driver]
            pass

        return driver, dict(user=user, passwd=passwd, host=host, db=db, port=port)

    def query(self, query, params: Union[tuple, dict] = None, commit=False) -> Union[MySQL, PostgreSQL]:
        """
        Dict use with placeholder %(kw)s.
        List/Tuple use with placeholder %s.

        :param query:
        :param Union[tuple, dict] params:
        :param commit:
        :rtype: Union[MySQL, PostgreSQL]
        :return:
        """
        return self._driver.query(query=query, params=params, commit=commit)

    def commit(self):
        self._driver.commit()
        pass
    
    def rollback(self):
        self._driver.rollback()
        pass
    
    def close(self):
        self._driver.close()
        pass

    def table(self, table: str):
        return SqlBuilder(self, table)

    def mogrify(self, query, params) -> str:
        return self._driver.mogrify(query=query, params=params)

    def inserted_id(self):
        if isinstance(self._driver, PostgreSQL):
            raise DatabaseException("PostgreSQL does not have .insert_id(), use ` RETURNING id;` "
                                    "on end of insert query.")
        elif isinstance(self._driver, MySQL):
            return self._driver.inserted_id()
        else:
            return None
        pass

    pass


class DatabaseException(Exception):
    pass
