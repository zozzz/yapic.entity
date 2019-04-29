from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF, Py_DECREF, Py_XINCREF, Py_XDECREF
from cpython.list cimport PyList_GET_ITEM, PyList_GET_SIZE
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM, PyTuple_GET_ITEM, _PyTuple_Resize

from yapic.entity._entity cimport EntityType, EntityState

from ._connection cimport Connection


cdef class QueryContext:
    def __cinit__(self, Connection conn, cursor_factory, list columns):
        self.conn = conn
        self.cursor_factory = cursor_factory
        self.columns = columns

        if not columns:
            raise ValueError("Columns must be not empty")

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

    cdef object convert_row(self, object row):
        cdef PyObject* columns = <PyObject*>self.columns
        cdef int length = PyList_GET_SIZE(<object>columns)
        # cdef tuple part
        cdef PyObject* col
        cdef PyObject* tmp_object

        if length == 1:
            col = PyList_GET_ITEM(<object>columns, 0)
            if isinstance(<object>col, EntityType):
                return create_entity(<EntityType>col, row, 0, len(row))

        cdef tuple result = PyTuple_New(length)
        tmp_object = <PyObject*>result
        cdef int c = 0
        cdef int fc

        for i in range(length):
            col = PyList_GET_ITEM(<object>columns, i)

            print(i, <object>col)

            if isinstance(<object>col, EntityType):
                fc = len((<EntityType>col).__fields__)
                entity = create_entity(<EntityType>col, row, c, c + fc)
                c += fc
                Py_INCREF(<object>entity)
                PyTuple_SET_ITEM(<object>tmp_object, i, <object>entity)
            else:
                tmp = row[i]
                Py_INCREF(<object>tmp)
                PyTuple_SET_ITEM(<object>tmp_object, i, <object>tmp)
                c += 1

        return result

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


# TODO: asyncpg record optimalizálás
cdef inline object create_entity(EntityType ent, object record, int start, int end):
    if record is None:
        return

    cdef int state_len = len(ent.__attrs__)
    cdef tuple data = PyTuple_New(state_len)
    cdef object item
    cdef int c = 0

    if end - start > state_len:
        raise RuntimeError("Too many columns")

    for i in range(start, end):
        item = record[i]
        Py_INCREF(<object>item)
        PyTuple_SET_ITEM(<object>data, c, <object>item)
        c += 1

    state = EntityState(ent, data)
    return ent(state)


# cdef inline object create_entity(EntityType ent, PyObject** values):
#     cdef int length = len(ent.__attrs__)

#     _PyTuple_Resize(values, length)
#     Py_INCREF(<object>values[0])

#     cdef EntityState state = EntityState(ent, <tuple>values[0])
#     return ent(state)

