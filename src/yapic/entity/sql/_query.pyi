from typing import Generic, Literal, Optional, Type, TypeVar, Union

from .._entity import Entity
from .._expression import Expression

ENT = TypeVar("ENT", bound=Entity)
JOIN = TypeVar("JOIN", bound=Entity)


class Query(Generic[ENT], Expression):
    def __init__(self, from_: Optional[Type[ENT]] = None):
        pass

    def select_from(self, from_: Type[ENT]) -> "Query[ENT]":
        pass

    def columns(self, *columns: Expression) -> "Query[ENT]":
        pass

    def where(self, *expr: Expression, **eq) -> "Query[ENT]":
        pass

    def order(self, *expr: Expression) -> "Query[ENT]":
        pass

    def group(self, *expr: Expression) -> "Query[ENT]":
        pass

    def having(self, *expr: Expression, **eq) -> "Query[ENT]":
        pass

    def distinct(self, *expr: Expression) -> "Query[ENT]":
        pass

    def prefix(self, *prefix: str) -> "Query[ENT]":
        pass

    def suffix(self, *suffix: str) -> "Query[ENT]":
        pass

    def join(self,
             what: Type[JOIN],
             condition: Optional[Expression] = None,
             type: Union[Literal["LEFT"], Literal["INNER"]] = "INNER") -> "Query[ENT]":
        pass

    def limit(self, count: int) -> "Query[ENT]":
        pass

    def offset(self, offset: int) -> "Query[ENT]":
        pass

    def for_update(self, *refs: Entity, nowait: bool = False, skip: bool = False) -> "Query[ENT]":
        pass

    def for_no_key_update(self, *refs: Entity, nowait: bool = False, skip: bool = False) -> "Query[ENT]":
        pass

    def for_share(self, *refs: Entity, nowait: bool = False, skip: bool = False) -> "Query[ENT]":
        pass

    def for_key_share(self, *refs: Entity, nowait: bool = False, skip: bool = False) -> "Query[ENT]":
        pass

    def reset_columns(self) -> "Query[ENT]":
        pass

    def reset_where(self) -> "Query[ENT]":
        pass

    def reset_order(self) -> "Query[ENT]":
        pass

    def reset_group(self) -> "Query[ENT]":
        pass

    def reset_range(self) -> "Query[ENT]":
        pass

    def reset_load(self) -> "Query[ENT]":
        pass

    def reset_exclude(self) -> "Query[ENT]":
        pass

    def reset_lock(self) -> "Query[ENT]":
        pass

    def load(self, *load) -> "Query[ENT]":
        pass

    def reduce_children(self, entities: set[Entity]) -> "Query[ENT]":
        """
        Reduce polymorph children query by polymorph ids
        """

    def exclude(self, *exclude) -> "Query[ENT]":
        pass

    def clone(self) -> "Query[ENT]":
        pass


