WrappyDatabase
==========

Python module with universal interface for PostgreSQL and MySQL.

```python
from Database import Database


# DSN
d = Database("scheme://user[:pass]@host[:port]/my_db")
# DSN for MySQL
mysql = Database("mysql://root@localhost/mysql")
# DSN for PostgreSQL
postgres = Database("postgresql://root:toot@dbsrv:5432/db")

# Or arguments
db = Database(host="host", usr="root", passwd="", db="test", driver=Database.POSTGRESQL)

# Use
db.query("SELECT * FROM my_tbl WHERE deleted = %s", [0]).fetch()
db.query("SELECT * FROM my_tbl WHERE deleted = %(deleted)s", {"deleted": 0}).fetch_all()

# SqlBuilder
db.table("my_tbl").fetch()
db.table("my_tbl").where("id IN %(ids)s", {"ids": [1, 2, 3]}).delete()
db.table("my_tbl").insert({
    "a": 1,
    "b": "string",
})
db.table("my_tbl").where("author = %s", [1]).update({
    "author": 2
})
```