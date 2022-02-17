import operator

from ._entity cimport EntityAttribute, EntityAttributeImpl, EntityBase, NOTSET
from ._expression cimport Expression, Visitor, BinaryExpression, ConstExpression, OrderExpression
from ._expression import asc, desc


cdef class VirtualAttribute(EntityAttribute):
    def __cinit__(self, *args, get, set=None, delete=None, compare=None, value=None, order=None):
        self._get = get
        self._set = set
        self._del = delete
        self._cmp = compare
        self._val = value
        self._order = order
        self._virtual_ = True

    def __get__(self, instance, owner):
        if instance is None:
            return self
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

    cdef object get_source(self):
        if self._source is not None:
            return self._source
        else:
            return self.get_entity()

    cdef VirtualAttribute with_path(self, PathExpression path):
        cdef VirtualAttribute result = self.clone()
        result._source = path
        result._uid_ = self._uid_
        return result

    cdef Expression get_value_expr(self, object query):
        if self._val is not None:
            return self._val(self.get_source(), query)
        else:
            raise ValueError(f"Can't use this attribute {self} as value in query")

    cdef Expression get_order_expr(self, object query, object op):
        if self._order is not None:
            return self._order(self.get_source(), query, op)
        return op(self.get_value_expr(query))

    cdef Expression get_compare_expr(self, object query, object op, object value):
        if self._cmp is not None:
            return self._cmp(self.get_source(), query, op, value)
        return op(self.get_value_expr(query), value)

    cdef BinaryExpression _new_binary_expr(self, object right, object op):
        return VirtualBinaryExpression(self, right, op)

    cpdef asc(self):
        return VirtualOrderExpression(self, True)

    cpdef desc(self):
        return VirtualOrderExpression(self, False)


cdef class VirtualAttributeImpl(EntityAttributeImpl):
    pass


cdef class VirtualBinaryExpression(BinaryExpression):
    cpdef Expression _create_expr_(self, object query):
        if self.op in (operator.__and__, operator.__or__):
            return BinaryExpression(self.left, self.right, self.op)

        # TODO: nem lehet mind a két oldal virtual

        cdef VirtualAttribute attr
        cdef object value

        if isinstance(self.left, VirtualAttribute):
            attr = <VirtualAttribute>self.left
            value = self.right
        elif isinstance(self.right, VirtualAttribute):
            attr = <VirtualAttribute>self.right
            value = self.left

        if isinstance(value, ConstExpression):
            value = (<ConstExpression>value).value

        return attr.get_compare_expr(query, self.op, value)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_virtual_binary(self)

    # def __getattr__(self, key):
    #     return getattr(self.left, key)

    def __repr__(self):
        return f"<VirtualBinary {self.left} {self.op} {self.right}>"


cdef class VirtualOrderExpression(OrderExpression):
    cpdef Expression _create_expr_(self, object query):
        if not isinstance(self.expr, VirtualAttribute):
            raise RuntimeError("Not implemented complex virtual order")

        return (<VirtualAttribute>self.expr).get_order_expr(query, asc if self.is_asc else desc)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_virtual_order(self)

    def __repr__(self):
        return f"<VirtualOrder {self.expr} {'ASC' if self.is_asc else 'DESC'}>"
