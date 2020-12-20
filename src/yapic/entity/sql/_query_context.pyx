from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF, Py_DECREF, Py_XINCREF, Py_XDECREF
from cpython.list cimport PyList_GET_ITEM, PyList_GET_SIZE
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM, PyTuple_GET_ITEM, _PyTuple_Resize

from yapic.entity._entity cimport EntityType, EntityState, EntityAttribute
from yapic.entity._field cimport StorageType
from yapic.entity._field_impl cimport CompositeImpl

from ._connection cimport Connection
from ._dialect cimport Dialect
from ._record_converter cimport RCState
from ._record_converter import convert_record


cdef class QueryContext:
    def __cinit__(self, Connection conn, cursor_factory, list rcos_list):
        self.conn = conn
        self.cursor_factory = cursor_factory
        self.rcos_list = rcos_list
        self.rc_state = RCState(conn)

    async def fetch(self, num=None, *, timeout=None):
        cdef list rows = []
        async for record in self:
            rows.append(record)
        return rows

    async def fetchrow(self, *, timeout=None):
        async with ensure_transaction(self.conn.conn):
            cursor = await self.cursor_factory
            row = await cursor.fetchrow(timeout=timeout)
            if row:
                return self.convert_row(row)
            else:
                return None

    async def forward(self, num, *, timeout=None):
        async with ensure_transaction(self.conn.conn):
            cursor = await self.cursor_factory
            return await cursor.forward(num, timeout=timeout)

    async def fetchval(self, column=0, *, timeout=None):
        async with ensure_transaction(self.conn.conn):
            cursor = await self.cursor_factory
            row = await cursor.fetchrow(timeout=timeout)
            return row[column]

    async def first(self, *, timeout=None):
        async with ensure_transaction(self.conn.conn):
            cursor = await self.cursor_factory
            row = await cursor.fetchrow(timeout=timeout)
            if row is not None:
                return self.convert_row(row)
            else:
                return None

    cdef convert_row(self, object row):
        return convert_record(row, self.rcos_list, self.rc_state)

    async def __aiter__(self):
        async with ensure_transaction(self.conn.conn):
            async for record in self.cursor_factory.__aiter__():
                yield self.convert_row(record)

    def __await__(self):
        return self.fetch().__await__()


cdef inline object ensure_transaction(conn):
    if conn._top_xact is None:
        return conn.transaction(isolation="serializable", readonly=True)
    else:
        return conn.transaction(
            isolation=conn._top_xact._isolation,
            readonly=conn._top_xact._readonly,
            deferrable=conn._top_xact._deferrable)
