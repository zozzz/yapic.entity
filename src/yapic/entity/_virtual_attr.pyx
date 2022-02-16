from ._entity cimport EntityAttribute, EntityAttributeImpl, EntityBase, NOTSET
from ._expression cimport Expression, Visitor, BinaryExpression, ConstExpression, VirtualExpressionVal, VirtualExpressionBinary


cdef class VirtualAttribute(EntityAttribute):
    def __cinit__(self, *args, get, set=None, delete=None, compare=None, value=None, order=None):
        self._get = get
        self._set = set
        self._del = delete
        self._cmp = compare
        self._val = value
        self._order = order

    def __get__(self, instance, owner):
        if instance is None:
            return VirtualExpressionVal(self, self.get_entity())
        elif isinstance(instance, EntityBase):
            res = (<EntityBase>instance).__state__.get_value(self)
            if res is NOTSET:
                return self._get(instance)
            else:
                return res
        else:
            raise RuntimeError()

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
        return type(self)(self._impl_,
            get=self._get,
            set=self._set,
            delete=self._del,
            compare=self._cmp,
            value=self._val,
            order=self._order)

    def compare(self, fn):
        self._cmp = fn
        return self

    def value(self, fn):
        self._val = fn
        return self

    def order(self, fn):
        self._order = fn
        return self

    def __repr__(self):
        return f"<Virtual {self._key_} {self._entity_repr()}>"

    cpdef visit(self, Visitor visitor):
        return visitor.visit_virtual_attr(self)


cdef class VirtualAttributeImpl(EntityAttributeImpl):
    pass



