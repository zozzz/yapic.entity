from ._entity cimport EntityAttribute, EntityAttributeImpl, EntityBase
from ._expression cimport Expression, Visitor, BinaryExpression, ConstExpression, VirtualExpression


cdef class VirtualAttribute(EntityAttribute):
    def __cinit__(self, *args, get, set=None, delete=None, compare=None, value=None):
        self._get = get
        self._set = set
        self._del = delete
        self._cmp = compare
        self._val = value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            return self._get(instance)

    def __set__(self, EntityBase instance, value):
        if self._set:
            self._set(instance, value)
        else:
            raise ValueError("Can't set attribute: '%s'" % self._key_)

    def __delete__(self, EntityBase instance):
        if self._del:
            self._del(instance)
        else:
            raise ValueError("Can't delete attribute: '%s'" % self._key_)

    cpdef clone(self):
        return type(self)(self._impl,
            get=self._get,
            set=self._set,
            delete=self._del,
            compare=self._cmp,
            value=self._val)

    def compare(self, fn):
        self._cmp = fn
        return self

    def value(self, fn):
        self._val = fn
        return self

    cdef BinaryExpression _new_binary_expr(self, object left, object other, object op):
        # print("VirtualAttribute", "VirtualExpression", left, other, op)
        return VirtualExpression(left, other, op)

    def __repr__(self):
        return "<virtual %s>" % self._key_

    cpdef visit(self, Visitor visitor):
        return visitor.visit_virtual_attr(self)


cdef class VirtualAttributeImpl(EntityAttributeImpl):
    pass



