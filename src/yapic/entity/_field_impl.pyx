from enum import Enum, Flag
from datetime import date, datetime

from ._entity cimport EntityType, EntityBase, EntityAttributeImpl, EntityAttribute, NOTSET
from ._expression cimport PathExpression, VirtualExpressionVal



cdef class StringImpl(FieldImpl):
    def __repr__(self):
        return "String"


cdef class BytesImpl(FieldImpl):
    def __repr__(self):
        return "Bytes"


cdef class ChoiceImpl(FieldImpl):
    def __cinit__(self, enum):
        if not issubclass(enum, Enum):
            raise TypeError("Choice type argument must be a subclass of Enum type")

        self._enum = enum
        self.is_multi = issubclass(enum, Flag)

    @property
    def enum(self):
        return self._enum

    def __repr__(self):
        return "Choice(%r)" % [item.value for item in self._enum]


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
        entity.__meta__["is_type"] = True
        self._entity_ = entity
        super().__init__()

    cpdef object init(self, EntityAttribute attr):
        attr._deps_.add(self._entity_)
        return True

    cpdef getattr(self, EntityAttribute attr, object key):
        obj = getattr(self._entity_, key)
        if isinstance(obj, VirtualExpressionVal):
            return VirtualExpressionVal((<VirtualExpressionVal>obj)._virtual_, PathExpression([attr]))
        else:
            return PathExpression([attr, obj])

    cdef object state_init(self, object initial):
        if initial is NOTSET:
            return self._entity_()
        else:
            return initial

    cdef object state_set(self, object initial, object current, object value):
        if value is None:
            return None
        elif current is NOTSET:
            if isinstance(value, EntityBase):
                return value
        elif isinstance(value, EntityBase):
            return value

        return self._entity_(value)

    cdef object state_get_dirty(self, object initial, object current):
        if current is NOTSET:
            if initial is NOTSET:
                return NOTSET
            elif initial.__state__.is_dirty:
                return initial
        elif initial is not current:
            return current
        elif current is not None and current.__state__.is_dirty:
            return current
        return NOTSET


cdef class JsonImpl(EntityTypeImpl):
    def __init__(self, entity):
        entity.__meta__["is_virtual"] = True
        super().__init__(entity)

    def __repr__(self):
        return "Json(%r)" % self._entity_


cdef class CompositeImpl(EntityTypeImpl):
    def __eq__(self, other):
        if isinstance(other, CompositeImpl):
            self_ent = entity_qname(self._entity_)
            other_ent = entity_qname((<CompositeImpl>other)._entity_)
            return self_ent == other_ent
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "Composite(%r)" % self._entity_


cdef class NamedTupleImpl(CompositeImpl):
    def __init__(self, entity):
        entity.__meta__["is_virtual"] = True
        super().__init__(entity)

    cpdef getattr(self, EntityAttribute attr, object key):
        return PathExpression([attr, getattr(self._entity_, key)._index_])

    def __repr__(self):
        return "NamedTuple(%r)" % self._entity_


cdef class AutoImpl(FieldImpl):
    def __repr__(self):
        return "Auto"


cdef str entity_qname(EntityType ent):
    try:
        schema = ent.__meta__["schema"]
    except KeyError:
        return ent.__name__
    else:
        if not schema or schema == "public":
            return ent.__name__
        else:
            return f"{schema}.{ent.__name__}"
