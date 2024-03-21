from inspect import iscoroutine
from logging import getLogger, DEBUG

from yapic.entity._entity cimport EntityType, EntityAttribute, NOTSET
from yapic.entity._entity_diff cimport EntityDiff
from yapic.entity._entity_operation cimport save_operations
from yapic.entity._entity_operation import EntityOperation
from yapic.entity._entity cimport EntityType, EntityBase, EntityState
from yapic.entity._entity import Entity
from yapic.entity._registry cimport Registry, RegistryDiff
from yapic.entity._field cimport Field, StorageType, PrimaryKey
from yapic.entity._field_impl cimport CompositeImpl, NamedTupleImpl
from yapic.entity._expression cimport Expression, PathExpression, RawExpression

from ._query cimport Query, QueryCompiler
from ._query_context cimport QueryContext
from ._dialect cimport Dialect


select_logger = getLogger("yapic.entity.sql.select")
insert_logger = getLogger("yapic.entity.sql.insert")
update_logger = getLogger("yapic.entity.sql.update")
delete_logger = getLogger("yapic.entity.sql.delete")


class Connection:
    def __init__(self, dialect):
        self.dialect = dialect

    def select(self, Query q, *, prefetch=None, timeout=None):
        cdef QueryCompiler qc = self.dialect.create_query_compiler()
        sql, params = qc.compile_select(q)

        if select_logger.isEnabledFor(DEBUG):
            select_logger.debug(f"{sql} {params}")

        # print("\n" + "=" * 50)
        # print(sql, params)
        # # print(q._load)
        # print("." * 50)
        # from pprint import pprint
        # pprint(qc.rcos_list)
        # print("=" * 50)

        return QueryContext(
            self,
            self.cursor(sql, *params, prefetch=prefetch, timeout=timeout),
            qc.rcos_list
        )

    # async def create_entity(self, EntityType ent, *, drop=False):
    #     raise NotImplementedError()

    async def insert(self, EntityBase entity):
        cdef EntityType ent = type(entity)
        cdef Dialect dialect = self.dialect
        cdef list attrs = []
        cdef list names = []
        cdef list values = []
        cdef list where = []

        await _collect_attrs(dialect, entity, True, attrs, names, values, where, None)

        q, p = dialect.create_query_compiler() \
            .compile_insert(ent, attrs, names, values, False)

        if not q:
            return False

        if insert_logger.isEnabledFor(DEBUG):
            insert_logger.debug(f"{q} {p}")

        return await self._exec_iou(q, p, entity, ent)

    async def insert_or_update(self, EntityBase entity):
        cdef EntityType ent = type(entity)
        cdef Dialect dialect = self.dialect
        cdef list attrs = []
        cdef list names = []
        cdef list values = []
        cdef list where = []

        await _collect_attrs(dialect, entity, True, attrs, names, values, where, None)

        q, p = dialect.create_query_compiler() \
            .compile_insert_or_update(ent, attrs, names, values, False)

        if not q:
            return entity

        if insert_logger.isEnabledFor(DEBUG):
            insert_logger.debug(f"{q} {p}")
        elif update_logger.isEnabledFor(DEBUG):
            update_logger.debug(f"{q} {p}")

        return await self._exec_iou(q, p, entity, ent)

    async def update(self, EntityBase entity):
        cdef EntityType ent = type(entity)
        cdef Dialect dialect = self.dialect
        cdef list attrs = []
        cdef list names = []
        cdef list values = []
        cdef list where = []

        await _collect_attrs(dialect, entity, False, attrs, names, values, where, None)

        q, p = dialect.create_query_compiler() \
            .compile_update(ent, attrs, names, values, where, False)

        if not q:
            return entity

        if update_logger.isEnabledFor(DEBUG):
            update_logger.debug(f"{q} {p}")

        return await self._exec_iou(q, p, entity, ent)

    async def delete(self, EntityBase entity):
        cdef EntityType ent = type(entity)
        cdef Dialect dialect = self.dialect
        cdef list attrs = []
        cdef list names = []
        cdef list values = []
        cdef list where = []

        await _collect_attrs(dialect, entity, False, attrs, names, values, where, None)

        q, p = dialect.create_query_compiler() \
            .compile_delete(ent, attrs, names, values, where, False)

        if not q:
            return False

        if delete_logger.isEnabledFor(DEBUG):
            delete_logger.debug(f"{q} {p}")

        return bool(await self._exec_del(q, p))

    async def _exec_iou(self, str q, params, EntityBase entity, EntityType entity_t):
        raise NotImplementedError()

    async def _exec_del(self, str q, params):
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
        cdef Registry reg = Registry()
        reg.is_draft = True
        reflect = self.dialect.create_ddl_reflect(base)
        await reflect.get_entities(self, reg)
        reg._finalize_entities()
        if reg.deferred:
            raise RuntimeError(f"Can't finalize all entities, remaining: {reg.deferred}")
        return reg

    def registry_diff(self, Registry a, Registry b, compare_field_position=True):
        return RegistryDiff(a, b, self.__entity_diff, compare_field_position=compare_field_position)

    def __entity_diff(self, a, b, compare_field_position):
        return self.dialect.entity_diff(a, b, compare_field_position)

    async def diff(self, Registry new_reg, EntityType entity_base=Entity, compare_field_position=True):
        registry = await self.reflect(entity_base)
        return self.registry_diff(registry, new_reg, compare_field_position=compare_field_position)




async def _collect_attrs(Dialect dialect, EntityBase entity, bint for_insert, list attrs, list names, list values, list where, Expression path):
    cdef EntityType entity_type = type(entity)
    cdef EntityState state = entity.__state__
    cdef EntityAttribute attr
    cdef Field field
    cdef list data = state.data_for_insert() if for_insert else state.data_for_update()
    cdef StorageType field_type
    # cdef bint has_nonpk_attr = False

    for i in range(len(data)):
        attr, value = <tuple>data[i]
        if isinstance(attr, Field) and attr._key_ is not None:
            if iscoroutine(value):
                value = await value

            if value is None:
                values.append(None)
            else:
                if isinstance(attr._impl_, CompositeImpl):
                    if not isinstance(value, EntityBase):
                        value = (<CompositeImpl>(<Field>attr)._impl_)._entity_(value)

                    value = (<CompositeImpl>(<Field>attr)._impl_).data_for_write(value, for_insert)

                    if isinstance(value, EntityBase):
                        if path is None:
                            spath = getattr(entity_type, attr._key_)
                        else:
                            spath = getattr(path, attr._key_)

                        # TODO: jobb megoldást találni arra, hogy felismerje azt,
                        #       hogy ez composite mezőt módosítani kell, de maga a composite mező nem dirty
                        #       mert egy másik lekérdezés composite mezője lett beállítva
                        #       - Asetleg az EntityTypeImpl.state_get_dirty függvényben kéne megjelölni a mezőket dirtyre
                        await _collect_attrs(dialect, value, True, attrs, names, values, where, spath)
                        continue

                values.append(dialect.encode_value(attr, value))

            attrs.append(attr)
            # if has_nonpk_attr is False and not attr.get_ext(PrimaryKey):
            #     has_nonpk_attr = True

            if path is None:
                names.append(dialect.quote_ident(attr._name_))
            else:
                names.append(_compile_path(dialect, getattr(path, attr._key_)))

    # Ha van változás, akkor a Field.on_update metódusát meghívja
    if not for_insert and len(data) > 0:
        for field in entity_type.__fields__:
            if field.on_update is not None:
                field_name = dialect.quote_ident(field._name_)
                if field_name not in names:
                    value = field.on_update(entity)
                    if iscoroutine(value):
                        value = await value

                    attrs.append(field)
                    names.append(dialect.quote_ident(field._name_))
                    values.append(dialect.encode_value(field, value))

    # Ha nem isnert, és nem valami composite type adatai kellenek, akkor összeállítja a where-t
    if not for_insert and not path:
        for attr in entity_type.__pk__:
            field_name = dialect.quote_ident(attr._name_)
            if field_name not in names:
                value = state.get_value(attr)
                if value is NOTSET:
                    raise RuntimeError("Missing primary key value")
                    # continue
            else:
                value = state.get_initial_value(attr)
                if value is NOTSET:
                    value = state.get_value(attr)
                    if value is NOTSET:
                        raise RuntimeError("Missing primary key value")

            pk_value = dialect.encode_value(attr, value)
            where.append((field_name, pk_value))

            # if primary key is not changed, remove from values
            try:
                existing_pk_idx = names.index(field_name)
            except ValueError:
                pass
            else:
                if values[existing_pk_idx] == pk_value:
                    attrs.pop(existing_pk_idx)
                    names.pop(existing_pk_idx)
                    values.pop(existing_pk_idx)


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

