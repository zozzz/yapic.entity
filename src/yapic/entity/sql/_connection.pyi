from ._query import Query
from ._query_context import QueryContext
from .._entity import EntityBase, EntityType, Entity
from .._registry import Registry, RegistryDiff


class Connection:
    def select(self, q: Query, *, prefetch=None, timeout=None) -> QueryContext:
        pass

    async def insert(self, entity: EntityBase) -> bool:
        pass

    async def insert_or_update(self, entity: EntityBase) -> bool:
        pass

    async def update(self, entity: EntityBase) -> bool:
        pass

    async def delete(self, entity: EntityBase) -> bool:
        pass

    async def save(self, entity: EntityBase) -> bool:
        pass

    async def reflect(self, base: EntityType = Entity) -> Registry:
        pass

    async def diff(self, new_reg: Registry, entity_base: EntityType = Entity) -> RegistryDiff:
        pass
