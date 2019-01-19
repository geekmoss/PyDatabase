from . import Database
from typing import Union


class SqlBuilder:
    """
    Module for building queries

    connection is DSN string or WrappyDatabase object
    """
    def __init__(self, database: Database, table: str):
        """
        :param Union[str, WrappyDatabase] connection:
        """
        self._table = f"`{table}`"
        self._driver = database

        self._select_cols = None
        self._where = None
        self._group_by = None
        self._order_by = None
        self._limit = None
        self._offset = None

        self._cur = None
        pass

    def select(self, cols: Union[str, list, tuple]):
        """
        Str: "col1, col2, colN"
        List/Tuple: ["col1", "col2", "colN"]

        :param cols:
        :return:
        """
        if type(cols) is str:
            cols = cols.split(',')
            pass
        self._select_cols = []
        for col in cols:
            # self._select_cols.append(f"`{col.strip()}`")
            self._select_cols.append(col.strip())
            pass
        return self

    def where(self, condition: str, params: Union[list, dict] = None):
        self._where = self._driver.mogrify(condition, params)
        return self

    def group_by(self, cols: Union[str, list, tuple]):
        self._group_by = []
        if type(cols) is str:
            cols = cols.split(',')
            pass

        for col in cols:
            self._group_by.append(f"{col.strip()}")
        return self

    def order_by(self, order_by):
        self._order_by = order_by
        return self

    def limit(self, limit: int, offset: int = 0):
        self._limit = limit
        self._offset = offset
        return self

    def insert(self, values: dict) -> int:
        query = f"INSERT INTO {self._table} ("
        for key in values:
            query += f"{key}, "
            pass

        query = query[:-2] + ") VALUES ("
        for key in values:
            query += f"%({key})s, "
            pass
        query = query[:-2] + ")"
        return self._driver.query(self._driver.mogrify(query, values), commit=True).row_count()

    def update(self, values: dict):
        query = f"UPDATE {self._table} SET "
        for key in values:
            query += f"{key}=%({key})s, "
            pass

        if self._where is not None:
            query = query[:-2] + f" WHERE {self._where}"
            pass
        else:
            query = query[:-2]
            pass

        return self._driver.query(self._driver.mogrify(query, values), commit=True).row_count()

    def delete(self):
        query = f"DELETE FROM {self._table}"
        if self._where is not None:
            query += f" WHERE {self._where}"
            pass

        return self._driver.query(query, commit=True).row_count()

    def _build_select(self):
        query = "SELECT "

        # SELECT Cols
        if self._select_cols is None:
            query += "*"
            pass
        else:
            cols = ""
            for col in self._select_cols:
                cols += f"{col}, "
                pass
            query += cols[:-2]
            pass

        # FROM
        if self._table is None:
            raise SqlBuilderException("A table must be defined")

        query += f" FROM {self._table} "

        # WHERE
        if self._where is not None:
            query += f"WHERE {self._where}"
            pass

        # GROUP BY
        if self._group_by is not None:
            query += " GROUP BY " + ", ".join(self._group_by)
            pass

        # ORDER BY
        if self._order_by is not None:
            query += f" ORDER BY {self._order_by}"
            pass

        # LIMIT
        if self._limit is not None:
            query += f" LIMIT {self._limit}"
            if self._offset is not None:
                query += f" OFFSET {self._offset}"
                pass
            pass

        return query

    # Fetch
    def fetch(self):
        if self._cur is None:
            query = self._build_select()
            self._cur = self._driver.query(query)
            pass
        return self._cur.fetch()

    def fetch_many(self, size: int = None):
        if self._cur is None:
            query = self._build_select()
            self._cur = self._driver.query(query)
            pass
        return self._cur.fetch_many(size)

    def fetch_all(self):
        if self._cur is None:
            query = self._build_select()
            self._cur = self._driver.query(query)
            pass
        return self._cur.fetch_all()

    # Aggr Functions

    def count(self, col: str = "*"):
        query = f"SELECT COUNT({col}) AS `count` FROM {self._table}"
        if self._where is not None:
            query += f"WHERE {self._where}"
            pass

        if self._group_by is not None:
            query += " GROUP BY"
            for group in self._group_by:
                query += f" {group}, "
                pass

            query = query[:-2]
            pass

        return self._driver.query(query).fetch()['count']

    def avg(self, col: str):
        query = f"SELECT AVG({col}) AS `avg` FROM {self._table}"
        if self._where is not None:
            query += f"WHERE {self._where}"
            pass

        if self._group_by is not None:
            query += " GROUP BY"
            for group in self._group_by:
                query += f" {group}, "
                pass

            query = query[:-2]
            pass

        return self._driver.query(query).fetch()['avg']

    def sum(self, col: str):
        query = f"SELECT SUM({col}) AS `sum` FROM {self._table}"
        if self._where is not None:
            query += f"WHERE {self._where}"
            pass

        if self._group_by is not None:
            query += " GROUP BY"
            for group in self._group_by:
                query += f" {group}, "
                pass

            query = query[:-2]
            pass

        return self._driver.query(query).fetch()['sum']

    def max(self, col: str):
        query = f"SELECT MAX({col}) AS `max` FROM {self._table}"
        if self._where is not None:
            query += f"WHERE {self._where}"
            pass

        if self._group_by is not None:
            query += " GROUP BY"
            for group in self._group_by:
                query += f" {group}, "
                pass

            query = query[:-2]
            pass

        return self._driver.query(query).fetch()['max']

    def min(self, col: str):
        query = f"SELECT MIN({col}) AS `min` FROM {self._table}"
        if self._where is not None:
            query += f"WHERE {self._where}"
            pass

        if self._group_by is not None:
            query += " GROUP BY"
            for group in self._group_by:
                query += f" {group}, "
                pass

            query = query[:-2]
            pass

        return self._driver.query(query).fetch()['min']
    pass


class SqlBuilderException(Exception):
    pass
