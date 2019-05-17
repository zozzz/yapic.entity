# flake8: noqa

from typing import Generic, TypeVar, Union, Optional, List, Tuple, Type, Any
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from ._entity import Entity
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
    NumericImpl,
    FloatImpl,
    JsonImpl as _JsonImpl,
    CompositeImpl as _CompositeImpl,
    AutoImpl,
)
from ._geom_impl import (
    PointType,
    PointImpl,
)

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
                 size: Union[int, Tuple[int, int], None] = None):
        pass


String = Field[StringImpl, str, str]
Bytes = Field[BytesImpl, bytes, bytes]
Bool = Field[BoolImpl, bool, int]
Date = Field[DateImpl, date, str]
DateTime = Field[DateTimeImpl, datetime, str]
DateTimeTz = Field[DateTimeTzImpl, datetime, str]
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


class CompositeImpl(Generic[EntityT], _CompositeImpl):
    _entity_: Type[EntityT]

    def __init__(self, entity: Type[EntityT]):
        super().__init__(entity)


class Composite(Generic[EntityT], Field[CompositeImpl[EntityT], EntityT, str]):
    pass


Point = Field[PointImpl, PointType, Any]
