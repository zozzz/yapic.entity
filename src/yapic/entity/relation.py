from typing import Generic, List, NoReturn, TypeVar, Type, Union, Optional

from . import _relation
from ._relation import Loading  # noqa

__all__ = "Relation", "Many", "One", "ManyAcross", "Loading"

# TO MOVE THIS CODE INTO CYTHON:
# https://github.com/cython/cython/issues/2753

Impl = TypeVar("Impl")
T = TypeVar("T")


class Relation(Generic[Impl, T], _relation.Relation):
    __slots__ = ()
    __impl__: Impl

    # def __new__(cls, *args, **kwargs):
    #     return _relation.Relation.__new__(_relation.Relation, *args, **kwargs)

    def __init__(self, impl: Impl = None, *, join: Optional[str] = None):
        pass
        # self.__get__ = _relation.Relation.__get__  # type: ignore
        # self.__set__ = _relation.Relation.__set__  # type: ignore

    def __get__(self, instance, owner) -> T:
        return _relation.Relation.__get__(self, instance, owner)

    def __set__(self, instance, value: Union[T, None]) -> NoReturn:
        _relation.Relation.__set__(self, instance, value)

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

    def __init__(self, joined: JoinedT, value: ValueStore, across: AcrossT):
        pass


class RelatedItem(Generic[T], _relation.RelatedItem):
    pass


class RelatedList(Generic[T], _relation.RelatedList):
    pass


class RelatedDict(Generic[T], _relation.RelatedDict):
    pass


class One(Generic[JoinedT], Relation[ManyToOne[Type[JoinedT], RelatedItem[JoinedT]], JoinedT]):
    __slots__ = ()


class Many(Generic[JoinedT], Relation[OneToMany[Type[JoinedT], RelatedList[JoinedT]], List[JoinedT]]):
    __slots__ = ()


class ManyAcross(Generic[AcrossT, JoinedT], Relation[ManyToMany[Type[JoinedT], Type[AcrossT], RelatedList[JoinedT]],
                                                     List[JoinedT]]):
    __slots__ = ()


# class Index(_field.Index):
#     def __init__(self):
#         pass

# class ForeignKey(_field.ForeignKey):
#     def __init__(self,
#                  field: Union[Field[Any, Any, Any], str],
#                  *,
#                  group: str = None,
#                  on_update: str = "CASCADE",
#                  on_delete: str = "UPDATE"):
#         pass
