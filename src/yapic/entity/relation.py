from typing import Generic, TypeVar, overload, List

from ._relation import Relation as _Releation, ManyToOne, OneToMany, ManyToMany

__all__ = "Relation", "Many", "One"

# TO MOVE THIS CODE INTO CYTHON:
# https://github.com/cython/cython/issues/2753

Impl = TypeVar("Impl")
T = TypeVar("T")


class Relation(Generic[Impl, T], _Releation):
    def __new__(cls, *args, **kwargs):
        return _Releation.__new__(_Releation, *args, **kwargs)

    def __init__(self):
        pass

    # TODO: move to stub file
    # @overload
    # def __get__(self, instance: None, owner: type) -> "Relation[Impl, T]":
    #     pass

    # @overload
    # def __get__(self, instance: Inst, owner: Type[Inst]) -> T:
    #     pass

    # def __get__(self, instance: Inst, owner: Type[Inst]) -> T:
    #     pass

    # def __set__(self, instance, value):
    #     pass


JoinedT = TypeVar("JoinedT")
CrossT = TypeVar("CrossT")


class One(Generic[JoinedT], Relation[OneToMany, JoinedT]):
    pass


class Many(Generic[JoinedT], Relation[ManyToOne, List[JoinedT]]):
    pass


class ManyAcross(Generic[CrossT, JoinedT], Relation[ManyToMany, List[JoinedT]]):
    pass
