from typing import Generic, TypeVar, List, get_type_hints

from xyz import CCC

T = TypeVar("T")


class GetTValue(Generic[T]):
    def __init__(self):
        pass


class X:
    A: "CCC"


String = GetTValue["X"]
fr = String.__args__[0]
fr2 = CCC.__args__[0]

print(String.__module__)
print(CCC.__module__)

print(GetTValue["D"].__args__[0].__module__)

# resolver = TypeResolver(String, resolved)

# for resolved in resolver:
#     resolved.attributes = {
#         "attr_name": ("<code_object>", globals, locals),  ## ha forward ref, akkor egy code object lesz
#         "attr_name": "<real type>"
#     }

#     resolved.init_posargs
#     resolved.init_kwargs

# Impl = TypeVar("Impl")
# T = TypeVar("T")

# class Relation(Generic[Impl, T]):
#     pass

# class OneToMany:
#     pass

# class ManyToOne:
#     pass

# class ManyToMany:
#     pass

# JoinedT = TypeVar("JoinedT")
# CrossT = TypeVar("CrossT")

# class One(Generic[JoinedT], Relation[OneToMany, JoinedT]):
#     pass

# class Many(Generic[JoinedT], Relation[ManyToOne, List[JoinedT]]):
#     pass

# class ManyAcross(Generic[CrossT, JoinedT], Relation[ManyToMany, List[JoinedT]]):
#     pass

# class TableA:
#     pass

# class TableB:
#     pass

# print(get_type_hints(ManyAcross[TableA, TableB]))
