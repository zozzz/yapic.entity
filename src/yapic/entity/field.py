from typing import Generic, TypeVar, Union, Optional, List, Tuple, Type, Any
from datetime import date, datetime
from enum import Enum
from ._field import Field as _Field, Index, ForeignKey, PrimaryKey
from ._field_impl import (
    StringImpl,
    IntImpl,
    ChoiceImpl as _ChoiceImpl,
    BoolImpl,
    DateImpl,
    DateTimeImpl,
    DateTimeTzImpl,
)

__all__ = [
    "Field", "String", "Bool", "Date", "DateTime", "DateTimeTz", "Int", "Serial", "Choice", "PrimaryKey", "Index",
    "ForeignKey"
]

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
Bool = Field[BoolImpl, bool, int]
Date = Field[DateImpl, date, str]
DateTime = Field[DateTimeImpl, datetime, str]
DateTimeTz = Field[DateTimeTzImpl, datetime, str]


class Int(Field[IntImpl, int, int]):
    def __new__(cls, *args, **kwargs):
        return Field.__new__(cls, *args, **kwargs)


class Serial(Int):
    def __new__(cls, *args, **kwargs):
        return Int.__new__(cls, *args, **kwargs) // PrimaryKey(auto_increment=True)


EnumT = TypeVar("EnumT", bound=Enum)


class ChoiceImpl(Generic[EnumT], _ChoiceImpl):
    enum: Type[EnumT]
    is_multi: bool

    def __init__(self, enum: Type[EnumT]):
        pass


class Choice(Generic[EnumT], Field[ChoiceImpl[EnumT], EnumT, Any]):
    pass
