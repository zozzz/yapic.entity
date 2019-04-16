from ._expression cimport Expression, Visitor, BinaryExpression, UnaryExpression, DirectionExpression, AliasExpression, CastExpression, CallExpression, RawExpression
from ._entity cimport EntityType, EntityAttribute
from ._field cimport Field


cdef class ReplacerBase(Visitor):
    def visit_binary(self, BinaryExpression expr):
        return expr.op(self.visit(expr.left), self.visit(expr.right))

    def visit_unary(self, UnaryExpression expr):
        return expr.op(self.visit(expr.expr))

    def visit_direction(self, DirectionExpression expr):
        if expr.is_asc:
            return self.visit(expr.expr).asc()
        else:
            return self.visit(expr.expr).desc()

    def visit_alias(self, AliasExpression expr):
        return self.visit(expr.expr).alias(expr.value)

    def visit_cast(self, CastExpression expr):
        return self.visit(expr.expr).cast(expr.type)

    def visit_call(self, CallExpression expr):
        return self.visit(expr.callable)(*[self.visit(a) for a in expr.args])

    def visit_raw(self, RawExpression expr):
        return expr

    def visit_field(self, expr):
        return expr


cdef class EntityReplacer(ReplacerBase):
    def __cinit__(self, EntityType what, EntityType to):
        self.what = what
        self.to = to

    def visit_field(self, Field expr):
        cdef EntityType fent = expr._entity_
        if fent is self.what:
            return getattr(self.to, expr._attr_name_in_class)
        else:
            return expr


cdef class FieldAssigner(ReplacerBase):
    def __cinit__(self, EntityType where_t, EntityBase where_o, dict data):
        self.where_t = where_t
        self.where_o = where_o
        self.data = data

    def visit_binary(self, BinaryExpression expr):
        left = self.visit(expr.left)
        right = self.visit(expr.right)
        cdef EntityAttribute attr
        cdef object value

        if not isinstance(left, Expression) or not isinstance(right, Expression):
            if isinstance(left, EntityAttribute):
                attr = <EntityAttribute>left
                value = right
            elif isinstance(right, EntityAttribute):
                attr = <EntityAttribute>right
                value = left
            else:
                return expr.op(left, right)

            if attr._entity_ is self.where_t:
                self.where_o.__state__.set_value(attr, value)
            else:
                return expr.op(left, right)
        else:
            return expr.op(left, right)

    def visit_field(self, Field expr):
        cdef EntityType ent = expr._entity_
        cdef EntityBase data

        try:
            data = self.data[ent]
        except KeyError:
            return expr
        else:
            return data.__state__.get_value(expr)



