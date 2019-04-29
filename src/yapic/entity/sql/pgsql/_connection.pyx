from inspect import iscoroutine

import cython

from yapic.entity._entity cimport EntityType, EntityBase, EntityAttribute, EntityState
from yapic.entity._field cimport Field

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
        cdef EntityAttribute attr
        cdef EntityState state = entity.__state__
        cdef list data = state.data_for_insert()
        cdef int data_length = len(data)
        cdef list fields = []
        cdef list field_names = []
        cdef list values = []
        cdef list subst = []
        cdef int i

        for i in range(data_length):
            attr, value = <tuple>data[i]
            if isinstance(attr, Field):
                if iscoroutine(value):
                    value = await value

                fields.append(attr)
                field_names.append(self.dialect.quote_ident(attr._name_))
                values.append(value)
                subst.append(f"${i + 1}")

        # INSERT INTO X (...) VALUES (...) RETURNING ...
        fields_str = ", ".join(field_names)
        q = ("INSERT INTO ", self.dialect.table_qname(ent),
             " (", fields_str, ") VALUES (", ", ".join(subst), ")")

        return await self.__exec_iou(q, values, entity, ent, timeout)

    async def insert_or_update(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef EntityAttribute attr
        cdef EntityState state = entity.__state__
        cdef tuple pk = entity.__pk__

        if not pk:
            return await self.insert(entity, timeout)

        cdef list pk_names = [self.dialect.quote_ident(attr._name_) for attr in ent.__pk__]
        cdef list fields = []
        cdef list field_names = []
        cdef list subst = []
        cdef list values = []
        cdef list updates = []
        cdef int i = 1

        for attr, value in state.data_for_insert():
            if isinstance(attr, Field):
                if iscoroutine(value):
                    value = await value

                field_name = self.dialect.quote_ident(attr._name_)
                fields.append(attr)
                field_names.append(field_name)
                values.append(value)
                subst.append(f"${i}")

                if field_name not in pk_names:
                    updates.append(f"{field_name}=${i}")

                i += 1

        # INSERT INTO X (...) VALUES (...) ON CONFLICT (pk) DO UPDATE SET field=value RETURNING ...

        fields_str = ", ".join(field_names)
        q = ("INSERT INTO ", self.dialect.table_qname(ent),
            " (", fields_str, ") VALUES (", ", ".join(subst), ")")

        if updates:
            q += (" ON CONFLICT (", ", ".join(pk_names), ")", " DO UPDATE SET ", ", ".join(updates))

        return await self.__exec_iou(q, values, entity, ent, timeout)

    async def update(self, EntityBase entity, timeout=None):
        cdef EntityType ent = type(entity)
        cdef EntityAttribute attr
        cdef EntityState state = entity.__state__
        cdef tuple pk = entity.__pk__

        if not pk:
            raise ValueError("Can't update entity without primary key")

        fields = []
        updates = []
        values = []
        i = 1
        for attr, value in state.data_for_update():
            if isinstance(attr, Field):
                if iscoroutine(value):
                    value = await value

                ident = self.dialect.quote_ident(attr._name_)
                fields.append(attr)
                updates.append(f"{ident}=${i}")
                values.append(value)
                i += 1

        if not fields:
            return False

        condition = []
        for k, attr in enumerate(ent.__pk__):
            condition.append(f"{self.dialect.quote_ident(attr._name_)}=${i}")
            values.append(pk[k])
            i += 1


        q = ("UPDATE ", self.dialect.table_qname(ent), " SET ",
             ", ".join(updates), " WHERE ", " AND ".join(condition))

        return await self.__exec_iou(q, values, entity, ent, timeout)

    async def __exec_iou(self, tuple q, values, EntityBase entity, EntityType entity_t, timeout):
        cdef list field_names = [self.dialect.quote_ident(a._name_) for a in entity_t.__fields__]
        cdef EntityState state = entity.__state__
        cdef EntityAttribute attr

        q += (" RETURNING ", ", ".join(field_names))

        self.conn._check_open()
        res = await self.conn._execute("".join(q), values, 0, timeout)
        record = res[0]

        for attr in entity_t.__fields__:
            state.set_value(attr, record[attr._index_])

        state.reset()

        return True

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
