from inspect import iscoroutine

import cython
from asyncpg import Record
from asyncpg.connection import Connection as AsyncPgConnection

from yapic.entity._entity cimport EntityType, EntityBase, EntityAttribute, EntityState, NOTSET
from yapic.entity._field cimport Field, StorageType, PrimaryKey
from yapic.entity._field_impl cimport CompositeImpl

from .._connection import Connection
from .._dialect cimport Dialect
from ._dialect cimport PostgreDialect


class PostgreConnection(AsyncPgConnection, Connection):
    def __init__(self, *args, **kwargs):
        AsyncPgConnection.__init__(self, *args, **kwargs)
        Connection.__init__(self, PostgreDialect())

    async def _exec_iou(self, str q, params, EntityBase entity, EntityType entity_t, *, timeout=None):
        cdef Dialect dialect = self.dialect
        cdef list field_names = [dialect.quote_ident(a._name_) for a in entity_t.__fields__]
        cdef EntityState state = entity.__state__
        cdef EntityAttribute attr

        q += f" RETURNING {', '.join(field_names)}"

        self._check_open()
        res = await self._execute(q, params, 0, timeout)
        if res:
            set_rec_on_entity(dialect, entity, entity_t, res[0])
            return True
        else:
            return False

    async def _exec_del(self, str q, params, *, timeout=None):
        self._check_open()
        _, res, _ = await self._execute(q, params, 0, timeout, return_status=True)
        return res and int(res[7:]) > 0

    # def select(self, Query q, *, prefetch=None, timeout=None):
    #     return select(self.conn, self.dialect, q, prefetch, timeout)

    # def insert(self, EntityBase entity):
    #     return insert(self.conn, self.dialect, entity)

    # def insert_or_update(self, EntityBase entity):
    #     return insert_or_update(self.conn, self.dialect, entity)

    # def update(self, EntityBase entity):
    #     return update(self.conn, self.dialect, entity)

    # def delete(self, EntityBase entity):
    #     return delete(self.conn, self.dialect, entity)


# async def select(object conn, Dialect dialect, Query q, prefetch, timeout):
#     cdef QueryCompiler qc = dialect.create_query_compiler()
#     sql, params = qc.compile_select(q)

#     if select_logger.isEnabledFor(DEBUG):
#         select_logger.debug(f"{sql} {params}")

#     return QueryContext(
#         self,
#         self.conn.cursor(sql, *params, prefetch=prefetch, timeout=timeout),
#         qc.rcos_list
#     )


# async def insert(object conn, Dialect dialect, EntityBase entity, timeout=None):
#     cdef EntityType ent = type(entity)
#     cdef list attrs = []
#     cdef list names = []
#     cdef list values = []

#     await _collect_attrs(dialect, entity, True, attrs, names, values, None)

#     q, p = dialect.create_query_compiler() \
#         .compile_insert(ent, attrs, names, values, False)

#     if not q:
#         return False

#     if insert_logger.isEnabledFor(DEBUG):
#         insert_logger.debug(f"{q} {p}")

#     return await exec_iou(conn, dialect, q, p, entity, ent, timeout)


# async def insert_or_update(object conn, Dialect dialect, EntityBase entity, timeout=None):
#     cdef EntityType ent = type(entity)
#     cdef list attrs = []
#     cdef list names = []
#     cdef list values = []

#     await _collect_attrs(dialect, entity, True, attrs, names, values, None)

#     q, p = dialect.create_query_compiler() \
#         .compile_insert_or_update(ent, attrs, names, values, False)

#     if not q:
#         return entity

#     if insert_logger.isEnabledFor(DEBUG):
#         insert_logger.debug(f"{q} {p}")
#     elif update_logger.isEnabledFor(DEBUG):
#         update_logger.debug(f"{q} {p}")

#     return await exec_iou(conn, dialect, q, p, entity, ent, timeout)


# async def update(object conn, Dialect dialect, EntityBase entity, timeout=None):
#     cdef EntityType ent = type(entity)
#     cdef list attrs = []
#     cdef list names = []
#     cdef list values = []

#     await _collect_attrs(dialect, entity, False, attrs, names, values, None)

#     q, p = dialect.create_query_compiler() \
#         .compile_update(ent, attrs, names, values, False)

#     if not q:
#         return entity

#     if update_logger.isEnabledFor(DEBUG):
#         update_logger.debug(f"{q} {p}")

#     return await exec_iou(conn, dialect, q, p, entity, ent, timeout)


# async def delete(object conn, Dialect dialect, EntityBase entity, timeout=None):
#     cdef EntityType ent = type(entity)
#     cdef list attrs = []
#     cdef list names = []
#     cdef list values = []

#     await _collect_attrs(dialect, entity, True, attrs, names, values, None)

#     q, p = dialect.create_query_compiler() \
#         .compile_delete(ent, attrs, names, values, False)

#     if not q:
#         return entity

#     if delete_logger.isEnabledFor(DEBUG):
#         delete_logger.debug(f"{q} {p}")

#     conn._check_open()
#     _, res, _ = await conn._execute(q, p, 0, timeout, return_status=True)
#     # _, res, _ = await self.conn._execute(q, p, 0, timeout, True)
#     return res and int(res[7:]) > 0


# async def exec_iou(object conn, Dialect dialect, str q, values, EntityBase entity, EntityType entity_t, timeout):
#     cdef list field_names = [dialect.quote_ident(a._name_) for a in entity_t.__fields__]
#     cdef EntityState state = entity.__state__
#     cdef EntityAttribute attr

#     q += f" RETURNING {', '.join(field_names)}"

#     # print(q)

#     conn._check_open()
#     res = await conn._execute(q, values, 0, timeout)
#     if res:
#         set_rec_on_entity(dialect, entity, entity_t, res[0])
#         return True
#     else:
#         return False


# TODO: refactor withoperations
cdef set_rec_on_entity(Dialect dialect, EntityBase entity, EntityType entity_t, record):
    cdef EntityState state = entity.__state__
    cdef EntityAttribute attr
    cdef StorageType field_type
    cdef CompositeImpl cimpl

    state.exists = True

    for k, v in record.items():
        attr = getattr(entity_t, k)
        if isinstance(v, Record):
            cimpl = attr._impl_
            nv = getattr(entity, k)
            if not isinstance(nv, EntityBase):
                nv = cimpl._entity_()
            set_rec_on_entity(dialect, nv, cimpl._entity_, v)
            v = nv

        if v is None:
            state.set_value(attr, None)
        else:
            field_type = dialect.get_field_type(attr)
            state.set_value(attr, field_type.decode(v))

    state.reset()
