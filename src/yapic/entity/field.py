# flake8: noqa

from typing import Generic, NoReturn, TypeVar, Union, Optional, List, Tuple, Type, Any, Callable
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from inspect import isfunction
import uuid

from ._entity import EntityAttribute, EntityAttributeExt, Entity
from ._field import Field as _Field, Index, ForeignKey, PrimaryKey, AutoIncrement
from ._field_impl import (
    StringImpl,
    BytesImpl,
    IntImpl,
    ChoiceImpl as _ChoiceImpl,
    BoolImpl,
    DateImpl,
    DateTimeImpl,
    DateTimeTzImpl,
    TimeImpl,
    TimeTzImpl,
    NumericImpl,
    FloatImpl,
    UUIDImpl,
    JsonImpl as _JsonImpl,
    CompositeImpl as _CompositeImpl,
    AutoImpl,
    ArrayImpl as _ArrayImpl,
)
from ._geom_impl import (
    PointType,
    PointImpl,
)
from ._virtual_attr import VirtualAttribute, VirtualAttributeImpl
from ._expression import const

Impl = TypeVar("Impl")
PyType = TypeVar("PyType")
RawType = TypeVar("RawType")

# TO MOVE THIS CODE INTO CYTHON:
# https://github.com/cython/cython/issues/2753


class Field(Generic[Impl, PyType, RawType], _Field):
    __impl__: Impl

    def __new__(cls, *args, **kwargs):
        return _Field.__new__(_Field, *args, **kwargs)

    def __init__(self,
                 impl: Impl = None,
                 *,
                 name: Optional[str] = None,
                 default: Optional[Union[PyType, RawType]] = None,
                 size: Union[int, Tuple[int, int], None] = None,
                 nullable: Optional[bool] = None,
                 on_update: Callable[[Entity], Any] = None):
        self.__get__ = _Field.__get__  # type: ignore
        self.__set__ = _Field.__set__  # type: ignore

    def __get__(self, instance, owner) -> PyType:
        pass

    def __set__(self, instance, value: Union[PyType, RawType, None]) -> NoReturn:
        pass


String = Field[StringImpl, str, str]
Bytes = Field[BytesImpl, bytes, bytes]
Bool = Field[BoolImpl, bool, int]
Date = Field[DateImpl, date, str]
DateTime = Field[DateTimeImpl, datetime, str]
DateTimeTz = Field[DateTimeTzImpl, datetime, str]
Time = Field[TimeImpl, time, str]
TimeTz = Field[TimeTzImpl, time, str]
Numeric = Field[NumericImpl, Decimal, str]
Auto = Field[AutoImpl, Any, Any]


class Int(Field[IntImpl, int, int]):

    def __new__(cls, *args, **kwargs):
        kwargs.setdefault("size", 4)
        return Field.__new__(cls, *args, **kwargs)


class Float(Field[FloatImpl, float, float]):

    def __new__(cls, *args, **kwargs):
        kwargs.setdefault("size", 4)
        return Field.__new__(cls, *args, **kwargs)


class UUID(Field[UUIDImpl, uuid.UUID, uuid.UUID]):

    def __new__(cls, *args, **kwargs):
        kwargs.setdefault("default", uuid.uuid4)
        return Field.__new__(cls, *args, **kwargs)


class Serial(Int):

    def __new__(cls, *args, **kwargs):
        return Int.__new__(cls, *args, **kwargs) // PrimaryKey() // AutoIncrement()


EnumT = TypeVar("EnumT", bound=Enum)


class ChoiceImpl(Generic[EnumT], _ChoiceImpl):
    enum: Type[EnumT]

    def __init__(self, enum: Type[EnumT]):
        super().__init__(enum)


class Choice(Generic[EnumT], Field[ChoiceImpl[EnumT], EnumT, Any]):

    def __new__(cls, impl, *args, **kwargs):
        field = Field.__new__(cls, impl, *args, **kwargs)
        return field // ForeignKey(impl.enum._entity_.value)


EntityT = TypeVar("EntityT")


class JsonImpl(Generic[EntityT], _JsonImpl):
    _entity_: Type[EntityT]

    def __init__(self, entity: Type[EntityT]):
        super().__init__(entity)


class Json(Generic[EntityT], Field[JsonImpl[EntityT], EntityT, str]):
    pass


class JsonArray(Generic[EntityT], Field[JsonImpl[List[EntityT]], List[EntityT], str]):
    pass


class CompositeImpl(Generic[EntityT], _CompositeImpl):
    _entity_: Type[EntityT]

    def __init__(self, entity: Type[EntityT]):
        super().__init__(entity)


class Composite(Generic[EntityT], Field[CompositeImpl[EntityT], EntityT, str]):
    pass


Point = Field[PointImpl, PointType, Any]


class ArrayImpl(Generic[Impl], _ArrayImpl):
    _item_impl_: Impl

    def __init__(self, item_impl: Impl):
        super().__init__(item_impl)


class Array(Generic[Impl, PyType, RawType], Field[ArrayImpl[Impl], PyType, RawType]):
    pass


StringArray = Array[StringImpl, List[str], List[str]]
IntArray = Array[IntImpl, List[int], List[int]]


class CreatedTime(Field[DateTimeTzImpl, datetime, datetime]):

    def __new__(cls, *args, **kwargs):
        kwargs.setdefault("default", const.CURRENT_TIMESTAMP)
        return Field.__new__(cls, *args, **kwargs)


class _UpdatedTimeExt(EntityAttributeExt):

    def init(self):
        # fontos a circular import miatt
        from .sql.pgsql._trigger import PostgreTrigger

        attr = self.attr
        entity = attr._entity_
        name = f"update-{attr._name_}"

        # TODO: entity.add_trigger() and check is already registered
        for trigger in entity.__triggers__:
            if trigger.name == name:
                # trigger already registered
                return

        when = f'OLD.* IS DISTINCT FROM NEW.* ' \
               f'AND (NEW."{attr._name_}" IS NULL OR OLD."{attr._name_}" = NEW."{attr._name_}")'

        entity.__triggers__.append(
            PostgreTrigger(
                name=name,
                before="UPDATE",
                for_each="ROW",
                when=when,
                body=f'NEW."{attr._name_}" = CURRENT_TIMESTAMP; RETURN NEW;',
            ))


class UpdatedTime(Field[DateTimeTzImpl, datetime, datetime]):

    def __new__(cls, *args, **kwargs):
        field = Field.__new__(cls, *args, **kwargs)
        return field // _UpdatedTimeExt()


def virtual(fn=None, *, depends: Optional[Union[list, tuple, str]] = None) -> VirtualAttribute:
    if fn is not None:
        return VirtualAttribute(VirtualAttributeImpl(), get=fn)
    else:
        if depends is not None:
            if isinstance(depends, str):
                depends = (depends, )
            elif not isinstance(depends, tuple):
                depends = tuple(depends)

        def factory(fn):
            return VirtualAttribute(VirtualAttributeImpl(), get=fn, depends=depends)

        return factory
