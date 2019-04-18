from yapic.entity._entity cimport EntityType
from yapic.entity._registry cimport Registry, RegistryDiff

from ._connection cimport Connection
from ._entity import Entity


async def sync(Connection connection, Registry registry, EntityType entity_base=Entity):
    cdef RegistryDiff diff = await connection.diff(registry, entity_base)

    if diff:
        return connection.dialect.create_ddl_compiler().compile_registry_diff(diff)
    else:
        return None
