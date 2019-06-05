from inspect import iscoroutine

from yapic.entity._entity cimport EntityType, EntityAttribute, NOTSET
from yapic.entity._entity_diff cimport EntityDiff
from yapic.entity._entity_operation cimport save_operations
from yapic.entity._entity_operation import EntityOperation
from yapic.entity._query cimport Query
from yapic.entity._entity cimport EntityType, EntityBase, EntityState
from yapic.entity._entity import Entity
from yapic.entity._registry cimport Registry, RegistryDiff
from yapic.entity._field cimport Field, StorageType
from yapic.entity._field_impl cimport CompositeImpl, NamedTupleImpl
from yapic.entity._expression cimport Expression, PathExpression, RawExpression

from ._query_context cimport QueryContext
from ._query_compiler cimport QueryCompiler
from ._dialect cimport Dialect


cdef class Connection:
    def __cinit__(self, conn, dialect):
        self.conn = conn
        self.dialect = dialect

        # self.fetch = self.conn.fetch
        # self.fetchrow = self.conn.fetchrow
        # self.fetchval = self.conn.fetchval
        # self.execute = self.conn.execute
        # self.executemany = self.conn.executemany
        # self.cursor = self.conn.cursor

    def select(self, Query q, *, prefetch=None, timeout=None):
        cdef QueryCompiler qc = self.dialect.create_query_compiler()
        sql, params = qc.compile_select(q)

        # print("\n" + "=" * 50)
        # print(sql)
        # from pprint import pprint
        # pprint(qc.rcos_list)
        # print("=" * 50)

        return QueryContext(
            self,
            self.conn.cursor(sql, *params, prefetch=prefetch, timeout=timeout),
            qc.rcos_list
        )

    # async def create_entity(self, EntityType ent, *, drop=False):
    #     raise NotImplementedError()

    async def insert(self, EntityBase entity):
        raise NotImplementedError()

    async def insert_or_update(self, EntityBase entity):
        raise NotImplementedError()

    async def update(self, EntityBase entity):
        raise NotImplementedError()

    async def delete(self, EntityBase entity):
        raise NotImplementedError()

    async def save(self, EntityBase entity):
        cdef EntityBase target
        cdef EntityBase src
        cdef bint res = False

        for op, param in save_operations(entity):
            if op is EntityOperation.REMOVE:
                res = await self.delete(param)
            elif op is EntityOperation.UPDATE:
                res = await self.update(param)
            elif op is EntityOperation.INSERT:
                res = await self.insert(param)
            elif op is EntityOperation.INSERT_OR_UPDATE:
                res = await self.insert_or_update(param)
            elif op is EntityOperation.UPDATE_ATTR:
                target = param[0]
                src = param[2]

                val = src.__state__.get_value(param[3])
                if val is not NOTSET:
                    target.__state__.set_value(param[1], val)

                res = True

            if not res:
                return False

        return True

    async def reflect(self, EntityType base=Entity):
        reg = Registry()
        reflect = self.dialect.create_ddl_reflect(base)
        await reflect.get_entities(self, reg)
        return reg

    def registry_diff(self, Registry a, Registry b):
        return RegistryDiff(a, b, self.__entity_diff)

    def __entity_diff(self, a, b):
        return self.dialect.entity_diff(a, b)

    async def diff(self, Registry new_reg, EntityType entity_base=Entity):
        registry = await self.reflect(entity_base)
        return self.registry_diff(registry, new_reg)

    async def _collect_attrs(self, EntityBase entity, bint for_insert, list attrs, list names, list values, Expression path=None):
        cdef EntityType entity_type = type(entity)
        cdef EntityState state = entity.__state__
        cdef EntityAttribute attr
        cdef list data = state.data_for_insert() if for_insert else state.data_for_update()
        cdef StorageType field_type

        for i in range(len(data)):
            attr, value = <tuple>data[i]
            if isinstance(attr, Field) and attr._key_ is not None:
                if iscoroutine(value):
                    value = await value

                if for_insert and isinstance(attr._impl_, NamedTupleImpl):
                    # TODO: beautify whole block...

                    if not isinstance(value, EntityBase):
                        value = (<CompositeImpl>(<Field>attr)._impl_)._entity_(value)

                    attrs.append(attr)

                    if path is None:
                        names.append(self.dialect.quote_ident(attr._name_))
                    else:
                        names.append(_compile_path(self.dialect, getattr(path, attr._key_)))

                    if value is None:
                        values.append(None)
                    else:
                        value = _make_tuple(self.dialect, value)
                        if value is None:
                            values.append(None)
                        else:
                            field_type = self.dialect.get_field_type(<Field>attr)
                            values.append(RawExpression(f"{field_type.name}{value}"))
                elif isinstance(attr._impl_, CompositeImpl):
                    if not isinstance(value, EntityBase):
                        value = (<CompositeImpl>(<Field>attr)._impl_)._entity_(value)

                    if path is None:
                        spath = getattr(entity_type, attr._key_)
                    else:
                        spath = getattr(path, attr._key_)

                    await self._collect_attrs(value, for_insert, attrs, names, values, spath)
                else:
                    attrs.append(attr)

                    if path is None:
                        names.append(self.dialect.quote_ident(attr._name_))
                    else:
                        names.append(_compile_path(self.dialect, getattr(path, attr._key_)))

                    if value is None:
                        values.append(None)
                    else:
                        field_type = self.dialect.get_field_type(<Field>attr)
                        values.append(field_type.encode(value))

        if not for_insert and not path:
            for attr in entity_type.__pk__:
                field_name = self.dialect.quote_ident(attr._name_)
                if field_name not in names:
                    value = state.get_value(attr)
                    if value is NOTSET:
                        continue

                    attrs.append(attr)
                    names.append(field_name)

                    if value is None:
                        values.append(None)
                    else:
                        field_type = self.dialect.get_field_type(attr)
                        values.append(field_type.encode(value))


cpdef wrap_connection(conn, dialect):
    if isinstance(dialect, str):
        package = __import__(f"yapic.entity.sql.{dialect}", fromlist=["Dialect", "Connection"])
        dialect = getattr(package, "Dialect")
        connection = getattr(package, "Connection")
    else:
        raise TypeError("Invalid dialect argument: %r" % dialect)
    return connection(conn, dialect())


cdef str _compile_path(Dialect dialect, PathExpression path):
    cdef list res = []

    for item in path._path_:
        if isinstance(item, Field):
            if len(res) == 0:
                res.append(dialect.quote_ident((<Field>item)._name_))
            else:
                res.append("." + dialect.quote_ident((<Field>item)._name_))
        elif isinstance(item, int):
            res.append(f"[{item}]")
        else:
            raise RuntimeError("Invalid path entry: %r" % item)

    return "".join(res)


cdef str _make_tuple(Dialect dialect, EntityBase value):
    cdef EntityType entity_type = type(value)
    cdef StorageType field_type
    cdef Field field
    cdef list res = []
    cdef bint is_null = True

    for field in entity_type.__fields__:
        if field._key_ is not None:
            field_type = dialect.get_field_type(field)
            v = getattr(value, field._key_)
            if v is None:
                res.append("NULL")
            else:
                is_null = False
                res.append(dialect.quote_value(field_type.encode(v)))

    if is_null:
        return None
    else:
        return f"({', '.join(res)})"

