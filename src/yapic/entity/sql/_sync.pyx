from inspect import iscoroutine

from yapic.entity._entity cimport EntityType, EntityState, EntityAttribute
from yapic.entity._entity import Entity
from yapic.entity._registry cimport Registry, RegistryDiff
from yapic.entity._registry import RegistryDiffKind
from yapic.entity._field cimport StorageType

from ._connection import _collect_attrs
from ._query cimport Query


async def sync(connection, Registry registry, EntityType entity_base=Entity, compare_field_position=True):
    if registry.deferred:
        raise RuntimeError(f"This registry is not fully resolved, some of entities deferred: {registry.deferred}")

    cdef RegistryDiff diff = await connection.diff(registry, entity_base, compare_field_position=compare_field_position)

    # print("\n".join(map(repr, diff.changes)))

    if diff:
        changes = []
        async for c in compare_data(connection, diff):
            async for cc in convert_data_to_raw(connection, c):
                changes.append(cc)
        diff.changes = changes

        res = connection.dialect.create_ddl_compiler().compile_registry_diff(diff)
        if not res:
            return None
        else:
            return res
    else:
        return None


async def compare_data(connection, RegistryDiff diff):
    for kind, param in diff:
        if kind is RegistryDiffKind.COMPARE_DATA:
            existing = await connection.select(Query().select_from(param[0]).columns(param[0]))
            for x in diff.compare_data(existing, param[1].__fix_entries__):
                yield x
        else:
            yield (kind, param)


async def convert_data_to_raw(connection, tuple change):
    cdef EntityType entity_t
    cdef list attrs
    cdef list names
    cdef list values

    kind, param = change
    if kind is RegistryDiffKind.INSERT_ENTITY or kind is RegistryDiffKind.UPDATE_ENTITY or kind is RegistryDiffKind.REMOVE_ENTITY:
        for entity_inst in param:
            entity_t = type(entity_inst)
            attrs = []
            names = []
            values = []
            where = []
            await _collect_attrs(connection.dialect, entity_inst, kind is RegistryDiffKind.INSERT_ENTITY, attrs, names, values, where, None)
            yield (kind, (entity_t, attrs, names, values, where))
    else:
        yield kind, param
