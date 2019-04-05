from ._expression cimport Expression, Visitor, BinaryExpression, UnaryExpression, DirectionExpression, AliasExpression, CastExpression
from ._entity cimport EntityType
from ._field cimport Field


cdef class EntityReplacer(Visitor):
    def __cinit__(self, EntityType what, EntityType to):
        self.what = what
        self.to = to

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

    def visit_field(self, Field expr):
        cdef EntityType fent = expr._entity_
        if fent is self.what:
            return getattr(self.to, expr._attr_name_in_class)
        else:
            return expr
