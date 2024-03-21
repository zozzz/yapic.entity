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
