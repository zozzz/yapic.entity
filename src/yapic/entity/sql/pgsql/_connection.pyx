from inspect import iscoroutine

import cython
from asyncpg import Record

from yapic.entity._entity cimport EntityType, EntityBase, EntityAttribute, EntityState, NOTSET
from yapic.entity._field cimport Field, StorageType, PrimaryKey
from yapic.entity._field_impl cimport CompositeImpl

from .._connection cimport Connection


cdef class PostgreConnection(Connection):
    async def create_entity(self, EntityType ent, *, drop=False):
        try:
            schema = ent.__meta__["schema"]
        except KeyError:
            pass
        else:
            await self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dialect.quote_ident(schema)}")

        qname = self.dialect.table_qname(ent)

        if drop:
            await self.conn.execute(f"DROP TABLE IF EXISTS {qname} CASCADE")

        ddl = self.dialect.create_ddl_compiler()
        return await self.conn.execute(ddl.compile_entity(ent))

    async def insert(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef list attrs = []
        cdef list names = []
        cdef list values = []
        cdef list placeholders = []

        await self.__collect_attrs(entity, True, "", attrs, names, values)

        if not values:
            return entity

        for i in range(1, len(names) + 1):
            placeholders.append(f"${i}")

        print(attrs)
        print(names)
        print(values)

        fields_str = ", ".join(names)
        placeholder_str = ", ".join(placeholders)

        q = ["INSERT INTO ", self.dialect.table_qname(ent),
            "(", fields_str, ") VALUES (", placeholder_str, ")"]

        return await self.__exec_iou(q, values, entity, ent, timeout)

    async def insert_or_update(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef list attrs = []
        cdef list names = []
        cdef list values = []
        cdef list updates = []
        cdef list placeholders = []
        cdef list pk_names = [self.dialect.quote_ident(attr._name_) for attr in ent.__pk__]

        await self.__collect_attrs(entity, True, "", attrs, names, values)

        if not values:
            return entity

        for i, name in enumerate(names):
            attr = <EntityAttribute>attrs[i]

            placeholders.append(f"${i+1}")

            if not attr.get_ext(PrimaryKey):
                updates.append(f"{name}=${i+1}")

        fields_str = ", ".join(names)
        placeholder_str = ", ".join(placeholders)

        q = ["INSERT INTO ", self.dialect.table_qname(ent),
            " (", fields_str, ") VALUES (", placeholder_str, ")"]

        if pk_names:
            q.extend([" ON CONFLICT (", ", ".join(pk_names), ") DO UPDATE SET ", ", ".join(updates)])

        return await self.__exec_iou(q, values, entity, ent, timeout)

    async def update(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef EntityAttribute attr
        cdef list attrs = []
        cdef list names = []
        cdef list values = []
        cdef list updates = []
        cdef list where = []

        if not entity.__pk__:
            raise ValueError("Can't update entity without primary key")

        await self.__collect_attrs(entity, False, "", attrs, names, values)

        if not values:
            return entity

        for i, name in enumerate(names):
            attr = <EntityAttribute>attrs[i]

            if attr.get_ext(PrimaryKey):
                where.append(f"{name}=${i+1}")
            else:
                updates.append(f"{name}=${i+1}")

        q = ["UPDATE ", self.dialect.table_qname(ent), " SET ",
            ", ".join(updates), " WHERE ", " AND ".join(where)]

        return await self.__exec_iou(q, values, entity, ent, timeout)

    async def __exec_iou(self, list q, values, EntityBase entity, EntityType entity_t, timeout):
        cdef list field_names = [self.dialect.quote_ident(a._name_) for a in entity_t.__fields__]
        cdef EntityState state = entity.__state__
        cdef EntityAttribute attr

        q.append(f" RETURNING {', '.join(field_names)}")

        # print("".join(q))

        self.conn._check_open()
        res = await self.conn._execute("".join(q), values, 0, timeout)
        record = res[0]

        self.__set_rec_on_entity(entity, entity_t, record)

        return True

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
            field_type = self.dialect.get_field_type(attr)
            state.set_value(attr, field_type.decode(v))

        state.reset()

    async def __collect_attrs(self, EntityBase entity, bint for_insert, str prefix, list attrs, list names, list values):
        cdef EntityType entity_type = type(entity)
        cdef EntityState state = entity.__state__
        cdef list data = state.data_for_insert() if for_insert else state.data_for_update()
        cdef StorageType field_type

        for i in range(len(data)):
            attr, value = <tuple>data[i]
            if isinstance(attr, Field):
                field_name = f"{prefix}{self.dialect.quote_ident(attr._name_)}"

                if iscoroutine(value):
                    value = await value

                if isinstance((<Field>attr)._impl_, CompositeImpl):
                    if not isinstance(value, EntityBase):
                        value = (<CompositeImpl>(<Field>attr)._impl_)._entity_(value)

                    await self.__collect_attrs(value, for_insert, f"{field_name}.", attrs, names, values)
                else:
                    field_type = self.dialect.get_field_type(<Field>attr)

                    attrs.append(attr)
                    names.append(field_name)
                    values.append(field_type.encode(value))

        if not for_insert and not prefix:
            for attr in entity_type.__pk__:
                field_name = self.dialect.quote_ident(attr._name_)
                if field_name not in names:
                    value = state.get_value(attr)
                    if value is NOTSET:
                        continue

                    field_type = self.dialect.get_field_type(attr)

                    attrs.append(attr)
                    names.append(field_name)
                    values.append(field_type.encode(value))


    async def delete(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef tuple pk = entity.__pk__

        if not pk:
            raise ValueError("Can't update entity without primary key")

        values = []
        condition = []
        for i, attr in enumerate(ent.__pk__):
            condition.append(f"{self.dialect.quote_ident(attr._name_)}=${i+1}")
            values.append(pk[i])
            # values.append(12)

        q = ("DELETE FROM ", self.dialect.table_qname(ent),
             " WHERE ", " AND ".join(condition))

        self.conn._check_open()
        _, res, _ = await self.conn._execute("".join(q), values, 0, timeout, True)
        return res and int(res[7:]) > 0
