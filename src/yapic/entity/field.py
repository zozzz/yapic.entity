from typing import Generic, TypeVar, Union, Optional, List, Tuple
from ._field import Field as _Field, StringImpl, IntImpl

__all__ = ["Field", "String", "Int"]

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


String = Field[StringImpl, str, bytes]
Int = Field[IntImpl, int, bytes]
