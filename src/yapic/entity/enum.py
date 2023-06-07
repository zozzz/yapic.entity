from enum import Enum as _Enum
from typing import Dict

from ._entity import Entity, EntityType
from .field import Int, PrimaryKey, String

_EnumMeta = type(_Enum)

REQUIRED_CLSDICT_FIELDS = frozenset(("__annotations__", "__module__", "__qualname__"))


class EntityEnumMeta(_EnumMeta):
    def __new__(cls, name, bases, clsdict, *, _entity=Entity, _root=False, **kwargs):
        new_ent = new_entity(name, bases, clsdict, _root=_root, _entity=_entity, **kwargs)

        if isinstance(clsdict._member_names, dict):
            member_names = tuple(clsdict._member_names.keys())

            def remove_member(key):
                try:
                    idx = member_names.index(key)
                except ValueError:
                    pass
                else:
                    del clsdict._member_names[key]
                    clsdict._last_values.pop(idx)

        else:
            # Python <= 3.10
            def remove_member(key):
                try:
                    idx = clsdict._member_names.index(key)
                except ValueError:
                    pass
                else:
                    clsdict._member_names.pop(idx)
                    clsdict._last_values.pop(idx)

        ignore = set()
        for field in new_ent.__attrs__:
            key = field._key_
            ignore.add(key)

            # azért kell eltávolítani, mert az _ignore_-nál gondot okozhat
            remove_member(key)

        clsdict["_ignore_"] = list(set(clsdict.get("_ignore_", [])) | ignore)
        dict.__setitem__(clsdict, "_entity_", new_ent)

        return _EnumMeta.__new__(cls, name, bases, clsdict)

    @staticmethod
    def __prepare__(cls, *args, **kwargs):
        return _EnumMeta.__prepare__(cls, *args)


def new_entity(name, bases, clsdict, *, _root, _entity=Entity, **kwargs):
    base_entity = _entity
    for base in bases:
        try:
            base_entity = base._entity_
        except AttributeError:
            pass
        else:
            break

    # for attr in base_entity.__attrs__:
    #     if attr._key_ not in clsdict:
    #         clsdict[attr._key_] = attr.clone()

    temp_removed = {}
    for k in list(clsdict.keys()):
        if k.startswith("_") and k not in REQUIRED_CLSDICT_FIELDS:
            temp_removed[k] = clsdict.pop(k)

    new_ent = EntityType(name, (base_entity,), clsdict, _root=_root, **kwargs)
    new_ent.__fix_entries__ = []

    for k, v in temp_removed.items():
        dict.__setitem__(clsdict, k, v)

    return new_ent


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
