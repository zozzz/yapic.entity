from inspect import iscoroutine

from yapic.entity._entity cimport EntityType, EntityState, EntityAttribute
from yapic.entity._registry cimport Registry, RegistryDiff
from yapic.entity._registry import RegistryDiffKind
from yapic.entity._query cimport Query

from ._connection cimport Connection
from ._entity import Entity


async def sync(Connection connection, Registry registry, EntityType entity_base=Entity):
    cdef RegistryDiff diff = await connection.diff(registry, entity_base)

    if diff:
        changes = []
        async for c in compare_data(connection, diff):
            changes.append(await convert_data_to_raw(c))
        diff.changes = changes

        return connection.dialect.create_ddl_compiler().compile_registry_diff(diff)
    else:
        return None


async def compare_data(Connection connection, RegistryDiff diff):
    for kind, param in diff:
        if kind is RegistryDiffKind.COMPARE_DATA:
            existing = await connection.select(Query().select_from(param).column(param))
            for x in diff.compare_data(existing, param.__fix_entries__):
                yield x
        else:
            yield (kind, param)


async def convert_data_to_raw(tuple change):
    cdef EntityType entity_t
    cdef EntityState state
    cdef EntityAttribute attr

    kind, param = change
    if kind is RegistryDiffKind.INSERT_ENTITY or kind is RegistryDiffKind.UPDATE_ENTITY:
        entity_t = type(param)
        state = param.__state__

        data = {}

        for attr, value in state.data_for_insert():
            if iscoroutine(value):
                value = await value
            data[attr._name_] = value

        return (kind, (entity_t, data))
    elif kind is RegistryDiffKind.REMOVE_ENTITY:
        entity_t = type(param)
        state = param.__state__

        data = {}

        for attr in entity_t.__pk__:
            value = state.get_value(attr)
            if iscoroutine(value):
                value = await value
            data[attr._name_] = value

        return (kind, (entity_t, data))
    else:
        return kind, param
