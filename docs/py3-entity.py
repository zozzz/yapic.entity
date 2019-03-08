from typing import Generic, Optional, overload, Type, TypeVar, Union, List, ClassVar
from mypy.plugin import Plugin
from mypy_extensions import TypedDict

PkType = TypeVar("PkType")


class Entity(Generic[PkType]):
    @classmethod
    def select(cls, *columns: Union[str, "Field"]) -> bytes:
        pass

    @classmethod
    def get(cls, *filter_expr, **filter_kw):
        pass

    @classmethod
    def get_pk(cls, pk: PkType):
        pass


PyWrap = TypeVar("PyWrap")
PyType = TypeVar("PyType")
DbType = TypeVar("DbType")
Ent = TypeVar("Ent", bound=Entity)

# def String() -> Type:
#     pass


class String:
    pass


class FieldMeta(type):
    def __instancecheck__(self, val):
        print("FieldMeta.__instancecheck__")
        return True


class Field(Generic[PyWrap, PyType, DbType], str):
    def __init__(self, default: Union[PyType, DbType, None] = None):
        pass

    @overload
    def __get__(self, instance: None, owner: Type[Ent]) -> "Field[PyWrap, PyType, DbType]":
        pass

    @overload
    def __get__(self, instance: Ent, owner: Type[Ent]) -> PyType:
        pass

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            return None

    def __set__(self, instance: Ent, value: Union[PyType, DbType]):

        pass

    def __del__(self, instance):
        pass

    def class_property(self) -> PyType:
        pass

    def eq(self, other):
        pass

    def __instancecheck__(self, val):
        print("Field.__instancecheck__")
        return True


class XXX(str):
    def xxx_method(self):
        pass


Stringf = Field[String, str, bytes]

TDTest = TypedDict("TDTest", {"name": Union[str, bytes]})


class User(Entity[int]):
    name: Stringf = "Hello"
    field: Stringf = Field()

    def aaa(self, **filter: TDTest) -> None:
        pass


# reveal_type(User.name)
# reveal_type(User().name)

User().name = "12"

print(isinstance("almafa", Field))
# print(isinstance("almafa", User.name))
print(User.name)
print(User.field)

reveal_type(User.name)
User().aaa(name="Fasza")
# reveal_type(User.select("OK"))
# reveal_type(User.get(User.name == "Almafa"))
# reveal_type(User.get(name="Almafa"))
# reveal_type(User.get_pk(10))
# reveal_type(User().get_pk(10))

# User.name.

# class User(Entity):
#     name: Optional[str] = None
#     without_value: str

#     constraint(name, Length(min=10, max=80))
#     # constraint(without_value, 100)
