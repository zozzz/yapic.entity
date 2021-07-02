from asyncpg.connection import Connection


class AsyncPGConnection(Connection):
    @property
    def conn(self):
        return self


