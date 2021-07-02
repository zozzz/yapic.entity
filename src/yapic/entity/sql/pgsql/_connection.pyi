from asyncpg.connection import Connection as AsyncPgConnection
from .._connection import Connection


class PostgreConnection(AsyncPgConnection, Connection):
    pass
