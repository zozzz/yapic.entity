from inspect import iscoroutine

import cython
from asyncpg import Record

from yapic.entity._entity cimport EntityType, EntityBase, EntityAttribute, EntityState, NOTSET
from yapic.entity._field cimport Field, StorageType, PrimaryKey
from yapic.entity._field_impl cimport CompositeImpl

from .._connection cimport Connection


cdef class PostgreConnection(Connection):
    async def insert(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef list attrs = []
        cdef list names = []
        cdef list values = []

        await self._collect_attrs(entity, True, "", attrs, names, values)

        q, p = self.dialect.create_query_compiler() \
            .compile_insert(ent, attrs, names, values, False)

        if not q:
            return entity

        return await self.__exec_iou(q, p, entity, ent, timeout)

    async def insert_or_update(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef list attrs = []
        cdef list names = []
        cdef list values = []

        await self._collect_attrs(entity, True, "", attrs, names, values)

        q, p = self.dialect.create_query_compiler() \
            .compile_insert_or_update(ent, attrs, names, values, False)

        if not q:
            return entity

        return await self.__exec_iou(q, p, entity, ent, timeout)

    async def update(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef list attrs = []
        cdef list names = []
        cdef list values = []

        await self._collect_attrs(entity, False, "", attrs, names, values)

        q, p = self.dialect.create_query_compiler() \
            .compile_update(ent, attrs, names, values, False)

        if not q:
            return entity

        return await self.__exec_iou(q, p, entity, ent, timeout)

    async def delete(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef list attrs = []
        cdef list names = []
        cdef list values = []

        await self._collect_attrs(entity, True, "", attrs, names, values)

        q, p = self.dialect.create_query_compiler() \
            .compile_delete(ent, attrs, names, values, False)

        if not q:
            return entity

        # print(q)

        self.conn._check_open()
        _, res, _ = await self.conn._execute(q, p, 0, timeout, True)
        return res and int(res[7:]) > 0

    async def __exec_iou(self, str q, values, EntityBase entity, EntityType entity_t, timeout):
        cdef list field_names = [self.dialect.quote_ident(a._name_) for a in entity_t.__fields__]
        cdef EntityState state = entity.__state__
        cdef EntityAttribute attr

        q += f" RETURNING {', '.join(field_names)}"

        # print(q)

        self.conn._check_open()
        res = await self.conn._execute(q, values, 0, timeout)
        record = res[0]

        self.__set_rec_on_entity(entity, entity_t, record)

        return True

    # TODO: refactor withoperations
    def __set_rec_on_entity(self, EntityBase entity, EntityType entity_t, record):
        cdef EntityState state = entity.__state__
        cdef EntityAttribute attr
        cdef StorageType field_type
        cdef CompositeImpl cimpl

        for k, v in record.items():
            attr = getattr(entity_t, k)
            if isinstance(v, Record):
                cimpl = attr._impl_
                nv = getattr(entity, k)
                if not isinstance(nv, EntityBase):
                    nv = cimpl._entity_()
                self.__set_rec_on_entity(nv, cimpl._entity_, v)
                v = nv

            if v is None:
                state.set_value(attr, None)
            else:
                field_type = self.dialect.get_field_type(attr)
                state.set_value(attr, field_type.decode(v))

        state.reset()
