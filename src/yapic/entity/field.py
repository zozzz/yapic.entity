# flake8: noqa

from typing import Generic, MutableMapping, TypeVar, TypedDict, Union, Optional, List, Tuple, Type, Any
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
import uuid

from ._entity import EntityAttribute, EntityAttributeExt
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
    JsonArrayImpl as _JsonArrayImpl,
    CompositeImpl as _CompositeImpl,
    AutoImpl,
    ArrayImpl as _ArrayImpl,
)
from ._geom_impl import (
    PointType,
    PointImpl,
)
from ._virtual_attr import VirtualAttribute, VirtualAttributeImpl
from ._expression import func, const

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
                 nullable: Optional[bool] = None):
        self.__get__ = _Field.__get__  # type: ignore

    def __get__(self, instance, owner) -> PyType:
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
    is_multi: bool

    def __init__(self, enum: Type[EnumT]):
        super().__init__(enum)


class Choice(Generic[EnumT], Field[ChoiceImpl[EnumT], EnumT, Any]):
    pass


EntityT = TypeVar("EntityT", bound=Enum)


class JsonImpl(Generic[EntityT], _JsonImpl):
    _entity_: Type[EntityT]

    def __init__(self, entity: Type[EntityT]):
        super().__init__(entity)


class Json(Generic[EntityT], Field[JsonImpl[EntityT], EntityT, str]):
    pass


class JsonArrayImpl(Generic[EntityT], _JsonArrayImpl):
    _entity_: Type[EntityT]

    def __init__(self, entity: Type[EntityT]):
        super().__init__(entity)


class JsonArray(Generic[EntityT], Field[JsonArrayImpl[EntityT], List[EntityT], str]):
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
    def init(self, attr: EntityAttribute):
        from .sql.pgsql._trigger import PostgreTrigger

        entity = attr._entity_
        trigger = PostgreTrigger(
            name=f"update-{attr._name_}",
            before="UPDATE",
            for_each="ROW",
            when=f"""OLD.* IS DISTINCT FROM NEW.*""",
            body=f"""
                NEW."{attr._name_}" = CURRENT_TIMESTAMP;
                RETURN NEW;
            """,
        )

        entity.__triggers__.append(trigger)


class UpdatedTime(Field[DateTimeTzImpl, datetime, datetime]):
    def __new__(cls, *args, **kwargs):
        field = Field.__new__(cls, *args, **kwargs)
        return field // _UpdatedTimeExt()


def virtual(fn) -> VirtualAttribute:
    return VirtualAttribute(VirtualAttributeImpl(), get=fn)
