from enum import Enum, Flag
from datetime import date, datetime

from ._entity cimport EntityType, EntityAttributeImpl, EntityAttribute
from ._expression cimport PathExpression


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


cdef class JsonImpl(FieldImpl):
    def __cinit__(self, entity):
        entity.__meta__["is_type"] = True
        entity.__meta__["is_virtual"] = True
        self._entity_ = entity

    cpdef object init(self, EntityAttribute attr):
        attr._deps_.add(self._entity_)

    cpdef getattr(self, EntityAttribute attr, object key):
        return PathExpression(attr, [getattr(self._entity_, key)])

    cpdef getitem(self, EntityAttribute attr, object index):
        return PathExpression(attr, [index])

    # def __eq__(self, other):
    #     if isinstance(other, JsonImpl):
    #         self_ent = entity_qname(self._entity_)
    #         other_ent = entity_qname((<JsonImpl>other)._entity_)
    #         return self_ent == other_ent
    #     else:
    #         return False

    # def __ne__(self, other):
    #     return not self.__eq__(other)

    def __repr__(self):
        return "Json(%r)" % self._entity_


cdef class CompositeImpl(FieldImpl):
    def __cinit__(self, entity):
        entity.__meta__["is_type"] = True
        self._entity_ = entity

    cpdef object init(self, EntityAttribute attr):
        attr._deps_.add(self._entity_)

    cpdef getattr(self, EntityAttribute attr, object key):
        return PathExpression(attr, [getattr(self._entity_, key)])

    cpdef getitem(self, EntityAttribute attr, object index):
        return PathExpression(attr, [index])

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
