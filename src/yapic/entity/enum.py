from enum import Enum as _Enum, EnumMeta as _EnumMeta

# from yapic.entity import String, Int, PrimaryKey, Entity, EntityType, Registry
from .field import String, Int, PrimaryKey
from ._entity import Entity, EntityType
from ._registry import Registry


class EntityEnumMeta(_EnumMeta):
    def __new__(cls, name, bases, clsdict, *, _entity=Entity, **kwargs):
        base_entity = _entity
        if bases:
            base_entities = [base._entity_ for base in bases if hasattr(base, "_entity_")]
            if base_entities:
                base_entity = base_entities[0]

        __init__ = clsdict.pop("__init__", None)
        __json__ = clsdict.pop("__json__", None)
        new_ent = EntityType(name, (base_entity, ), clsdict, **kwargs)
        if __init__ is not None:
            dict.__setitem__(clsdict, "__init__", __init__)
        if __json__ is not None:
            dict.__setitem__(clsdict, "__json__", __json__)

        new_ent.__fix_entries__ = []

        ignore = []
        for field in new_ent.__fields__:
            key = field._key_
            ignore.append(key)
            try:
                idx = clsdict._member_names.index(key)
            except ValueError:
                continue
            else:
                clsdict._member_names.pop(idx)
                clsdict._last_values.pop(idx)

        cignore = clsdict.get("_ignore_", [])
        cignore.extend(ignore)

        clsdict["_ignore_"] = ignore

        dict.__setitem__(clsdict, "_entity_", new_ent)

        return _EnumMeta.__new__(cls, name, bases, clsdict)

    @staticmethod
    def __prepare__(cls, *args, **kwargs):
        return _EnumMeta.__prepare__(cls, *args)


class Enum(_Enum, _root=True, metaclass=EntityEnumMeta):
    value: String = PrimaryKey()
    label: String
    index: Int

    def __init__(self, *args):
        _Enum.__init__(self)

        value_dict = {"value": self._name_}

        self.__update_seq__(value_dict)

        if len(args) == 1:
            if isinstance(args[0], dict):
                value_dict.update(args[0])
            else:
                value_dict["label"] = args[0]
        else:
            for field in self._entity_.__fields__:
                if len(args) > field._index_:
                    value_dict[field._name_] = args[field._index_]

        inst = self._entity_(value_dict)

        for k, v in value_dict.items():
            if k == "value":
                k = "_value_"
            setattr(self, k, v)

        self._entity_.__fix_entries__.append(inst)
        self._inst_ = inst

        type(self)._value2member_map_[inst.value] = self

    def __update_seq__(self, value_dict: dict):
        last_index = self._entity_.__fix_entries__[-1].index if self._entity_.__fix_entries__ else -1
        value_dict["index"] = last_index + 1

    def __json__(self):
        return self._inst_


# R = Registry()

# class X(Enum, _root=True, registry=R, schema="enum"):
#     pass

# class Test(X):
#     RUNNING = "Running"
#     PAUSED = "Paused"

# class Test2(Enum):
#     value: Int = PrimaryKey()

#     RUNNING_2 = "Running"
#     PAUSED_2 = "Paused"
#     EXTRA = ("CustomId", "Custom Label", 10)
#     VALAMI = dict(label="CSODA LABEL")

# print(Test._entity_.__fields__)
# print(Test2._entity_.__fields__)
