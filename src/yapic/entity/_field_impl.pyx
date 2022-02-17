from enum import Enum, Flag
from datetime import date, datetime
from inspect import iscoroutine
from typing import List

from ._entity cimport EntityType, EntityBase, EntityAttributeImpl, EntityAttribute, NOTSET
from ._expression cimport PathExpression, CallExpression, RawExpression
from ._field cimport StorageType, ForeignKey
from ._resolve cimport ResolveContext


cdef class FieldImpl(EntityAttributeImpl):
    def __eq__(self, other):
        while isinstance(other, AutoImpl):
            other = (<AutoImpl>other)._ref_impl
        return super().__eq__(other)


cdef class StringImpl(FieldImpl):
    def __repr__(self):
        return "String"


cdef class BytesImpl(FieldImpl):
    def __repr__(self):
        return "Bytes"


cdef class IntImpl(FieldImpl):
    def __repr__(self):
        return "Int"


cdef class BoolImpl(FieldImpl):
    def __repr__(self):
        return "Bool"


cdef class DateImpl(FieldImpl):
    def __repr__(self):
        return "Date"


cdef class DateTimeImpl(FieldImpl):
    def __repr__(self):
        return "DateTime"


cdef class DateTimeTzImpl(FieldImpl):
    def __repr__(self):
        return "DateTimeTz"


cdef class TimeImpl(FieldImpl):
    def __repr__(self):
        return "Time"


cdef class TimeTzImpl(FieldImpl):
    def __repr__(self):
        return "TimeTz"


cdef class NumericImpl(FieldImpl):
    def __repr__(self):
        return "Numeric"

cdef class FloatImpl(FieldImpl):
    def __repr__(self):
        return "Float"


cdef class UUIDImpl(FieldImpl):
    def __repr__(self):
        return "UUID"


cdef class EntityTypeImpl(FieldImpl):
    def __init__(self, entity):
        entity.set_meta("is_type", True)
        self._entity_ = entity
        super().__init__()

    cpdef object init(self, EntityAttribute attr):
        attr._deps_.add_entity(self._entity_)
        return True

    cpdef getattr(self, EntityAttribute attr, object key):
        obj = getattr(self._entity_, key)
        return PathExpression([attr, obj])

    cdef object state_init(self, object initial):
        if initial is NOTSET:
            return self._entity_()
        else:
            return initial

    cdef object state_set(self, object initial, object current, object value):
        if value is None:
            return None
        elif isinstance(value, EntityBase):
            return value
        else:
            return self._entity_(value)

    cdef object state_get_dirty(self, object initial, object current):
        if current is NOTSET:
            return NOTSET
        elif initial is NOTSET:
            return current
        elif isinstance(current, self._entity_) and current is initial:
            if current.__state__.is_dirty:
                return current
            else:
                return NOTSET
        elif not json_eq(initial, current):
            return current

        return NOTSET


cdef class JsonImpl(FieldImpl):
    def __init__(self, type = None):
        super().__init__()

        if isinstance(type, EntityType):
            self._object_ = type
            (<EntityType>type).set_meta("is_virtual", True)
        elif hasattr(type, "__origin__") and type.__origin__ is list:
            args = type.__args__
            if len(args) == 1:
                item_type = args[0]
                if isinstance(item_type, EntityType):
                    self._list_ = item_type
                    (<EntityType>item_type).set_meta("is_virtual", True)
                else:
                    self._any_ = True
            else:
                self._any_ = True
        else:
            self._any_ = True


    cpdef object init(self, EntityAttribute attr):
        if self._object_:
            attr._deps_.add_entity(self._object_)
        if self._list_:
            attr._deps_.add_entity(self._list_)
        return True

    cpdef getattr(self, EntityAttribute attr, object key):
        if self._object_:
            obj = getattr(self._object_, key)
            return PathExpression([attr, obj])
        else:
            return PathExpression([attr, key])

    cpdef getitem(self, EntityAttribute attr, object key):
        return PathExpression([attr, key])

    cdef object state_init(self, object initial):
        if initial is NOTSET:
            if self._object_:
                return self._object_()
            elif self._list_:
                return []
            else:
                return NOTSET
        else:
            return initial

    cdef object state_set(self, object initial, object current, object value):
        if value is None:
            return None
        elif self._object_:
            if isinstance(value, EntityBase):
                return value
            else:
                return self._object_(value)
        elif self._list_:
            return [self.__list_item(v) for v in value]
        else:
            return value

    cdef object state_get_dirty(self, object initial, object current):
        if current is NOTSET:
            return NOTSET
        elif initial is NOTSET:
            return current
        elif initial is current:
            if self.__check_dirty(current):
                return current
            else:
                return NOTSET
        elif not json_eq(initial, current):
            return current

        # if current is NOTSET:
        #     if initial is NOTSET:
        #         return NOTSET
        #     elif self._object_ or self._list_:
        #         if self.__check_dirty(initial):
        #             return initial
        # elif initial is not current:
        #     if not json_eq(initial, current):
        #         return current
        # elif self.__check_dirty(current):
        #     return current

        return NOTSET


    def __repr__(self):
        return "Json"

    cdef bint __check_dirty(self, object value):
        if isinstance(value, EntityBase):
            return (<EntityBase>value).__state__.is_dirty
        elif isinstance(value, list):
            for v in value:
                if self.__check_dirty(v):
                    return True
        else:
            return False

    cdef object __list_item(self, object value):
        if isinstance(value, EntityBase):
            return value
        else:
            return self._list_(value)

cdef bint json_eq(object a, object b):
    if isinstance(a, list) and isinstance(b, list):
        a_len = len((<list>a))
        b_len = len((<list>b))
        if a_len != b_len:
            return False
        else:
            for i in range(0, a_len):
                if not json_eq((<list>a)[i], (<list>b)[i]):
                    return False
            return True
    elif isinstance(a, dict) and isinstance(b, dict):
        return a == b
    elif isinstance(a, EntityBase) and isinstance(b, EntityBase):
        return a.__state__ == b.__state__
    elif isinstance(a, dict):
        if isinstance(b, EntityBase):
            return json_eq(a, b.as_dict())
        else:
            return False
    elif isinstance(b, dict):
        if isinstance(a, EntityBase):
            return json_eq(a.as_dict(), b)
        else:
            return False
    elif a is None or a is NOTSET:
        if isinstance(b, EntityBase):
            return (<EntityBase>b).__state__._is_empty() is True
        else:
            return a == b
    elif b is None or b is NOTSET:
        if isinstance(a, EntityBase):
            return (<EntityBase>a).__state__._is_empty() is True
        else:
            return a == b
    else:
        return a == b


cdef class CompositeImpl(EntityTypeImpl):
    cdef bint _is_eq(self, object other):
        if isinstance(other, CompositeImpl):
            self_ent = self._entity_.__qname__
            other_ent = (<CompositeImpl>other)._entity_.__qname__
            return self_ent == other_ent
        else:
            return False

    cpdef object data_for_write(self, EntityBase value, bint for_insert):
        return value

    def __repr__(self):
        return "Composite(%r)" % self._entity_


cdef class NamedTupleImpl(CompositeImpl):
    def __init__(self, EntityType entity):
        entity.set_meta("is_virtual", True)
        super().__init__(entity)

    cpdef getattr(self, EntityAttribute attr, object key):
        return PathExpression([attr, getattr(self._entity_, key)._index_])

    cpdef object data_for_write(self, EntityBase value, bint for_insert):
        raise NotImplementedError()

    def __repr__(self):
        return "NamedTuple(%r)" % self._entity_


cdef class AutoImpl(FieldImpl):
    def __repr__(self):
        return repr(self._ref_impl)

    cdef bint _is_eq(self, object other):
        return self._ref_impl == other

    # TODO:
    # cdef object _resolve_deferred(self, ResolveContext ctx, EntityAttribute attr):
    #     if FieldImpl._resolve_deferred(self, ctx, attr) is False:
    #         return False

    #     cdef ForeignKey fk = attr.get_ext(ForeignKey)
    #     if fk is None:
    #         raise ValueError("Can't use 'Auto' type without 'ForeignKey'")

    #     if fk._resolve_deferred(ctx) is False:
    #         return False

    #     # fk not resolved yet
    #     if fk.ref is None:
    #         return False

    #     self._ref_impl = fk.ref._impl_

    #     if isinstance(attr, Field):
    #         (<Field>attr).min_size = fk.ref.min_size
    #         (<Field>attr).max_size = fk.ref.max_size
    #     return True


cdef class ChoiceImpl(AutoImpl):
    def __init__(self, enum):
        if not issubclass(enum, Enum):
            raise TypeError("Choice type argument must be a subclass of Enum type")

        self._enum = enum

    @property
    def enum(self):
        return self._enum

    cdef object state_set(self, object initial, object current, object value):
        return self._coerce(value)

    cdef object state_get_dirty(self, object initial, object current):
        if current is NOTSET:
            return NOTSET
        elif initial is NOTSET:
            return current
        else:
            if self._coerce(initial) == self._coerce(current):
                return NOTSET
            else:
                return current

    # def __repr__(self):
    #     return "Choice(%r)" % [item.value for item in self._enum]

    cdef object _coerce(self, object value):
        if value is None:
            return None
        elif isinstance(value, self._enum):
            return value
        else:
            return self._enum(value)


cdef class ArrayImpl(FieldImpl):
    def __init__(self, item_impl):
        self._item_impl_ = item_impl
        super().__init__()

    cdef object state_init(self, object initial):
        if initial is NOTSET or initial is None:
            return []
        else:
            return list(initial)

    cpdef getitem(self, EntityAttribute attr, object index):
        if not isinstance(index, int):
            raise ValueError("Currently only int index is supported")
        return PathExpression([attr, index + 1])

    cdef object state_set(self, object initial, object current, object value):
        if value is NOTSET:
            return NOTSET
        elif value is None:
            return None
        else:
            return list(value)

    cdef object state_get_dirty(self, object initial, object current):
        if current is NOTSET:
            if initial is NOTSET:
                return NOTSET
            else:
                return initial
        elif initial != current:
            return current

        return NOTSET

    cdef bint _is_eq(self, object other):
        return isinstance(other, ArrayImpl) and (<ArrayImpl>other)._item_impl_ == self._item_impl_

    def __repr__(self):
        return f"Array({self._item_impl_})"
