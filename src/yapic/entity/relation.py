from typing import Generic, TypeVar, overload, Type

from . import _relation

__all__ = "Relation", "Many", "One", "ManyAcross"

# TO MOVE THIS CODE INTO CYTHON:
# https://github.com/cython/cython/issues/2753

Impl = TypeVar("Impl")
T = TypeVar("T")


class Relation(Generic[Impl, T], _relation.Relation):
    __slots__ = ()
    __impl__: Impl

    # def __new__(cls, *args, **kwargs):
    #     return _relation.Relation.__new__(_relation.Relation, *args, **kwargs)

    def __init__(self, impl: Impl):
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
AcrossT = TypeVar("AcrossT")
ValueStore = TypeVar("ValueStore")


class OneToMany(Generic[JoinedT, ValueStore], _relation.OneToMany):
    joined: JoinedT
    value: ValueStore

    def __new__(cls, *args, **kwargs):
        return _relation.OneToMany.__new__(_relation.OneToMany, *args, **kwargs)

    def __init__(self, joined: JoinedT, value: ValueStore):
        pass


class ManyToOne(Generic[JoinedT, ValueStore], _relation.ManyToOne):
    joined: JoinedT
    value: ValueStore

    def __new__(cls, *args, **kwargs):
        return _relation.ManyToOne.__new__(_relation.ManyToOne, *args, **kwargs)

    def __init__(self, joined: JoinedT, value: ValueStore):
        pass


class ManyToMany(Generic[JoinedT, AcrossT, ValueStore], _relation.ManyToMany):
    joined: JoinedT
    across: AcrossT
    value: ValueStore

    def __new__(cls, *args, **kwargs):
        return _relation.ManyToMany.__new__(_relation.ManyToMany, *args, **kwargs)

    def __init__(self, joined: JoinedT, across: AcrossT, value: ValueStore):
        pass


class RelatedItem(Generic[T], _relation.RelatedItem):
    pass


class RelatedList(Generic[T], _relation.RelatedList):
    pass


class RelatedDict(Generic[T], _relation.RelatedDict):
    pass


class One(Generic[JoinedT], Relation[ManyToOne[Type[JoinedT], RelatedItem[JoinedT]], JoinedT]):
    __slots__ = ()


class Many(Generic[JoinedT], Relation[OneToMany[Type[JoinedT], RelatedList[JoinedT]], RelatedList[JoinedT]]):
    __slots__ = ()


class ManyAcross(Generic[AcrossT, JoinedT],
                 Relation[ManyToMany[Type[JoinedT], Type[AcrossT], RelatedList[JoinedT]], RelatedList[JoinedT]]):
    __slots__ = ()
