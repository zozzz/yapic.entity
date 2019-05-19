from ._expression cimport Expression, Visitor, BinaryExpression, UnaryExpression, DirectionExpression, AliasExpression, CastExpression, CallExpression, RawExpression, PathExpression, coerce_expression
from ._entity cimport EntityType, EntityAttribute
from ._field cimport Field, field_eq
from ._relation cimport Relation


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

    def visit_path(self, PathExpression expr):
        return PathExpression(self.visit(expr._primary_), [self.visit(a) for a in expr._path_])


cdef class Walk(Visitor):
    def visit_binary(self, BinaryExpression expr):
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_unary(self, UnaryExpression expr):
        self.visit(expr.expr)

    def visit_direction(self, DirectionExpression expr):
        self.visit(expr.expr)

    def visit_alias(self, AliasExpression expr):
        self.visit(expr.expr)

    def visit_cast(self, CastExpression expr):
        self.visit(expr.expr)

    def visit_call(self, CallExpression expr):
        for a in expr.args:
            self.visit(a)

    def visit_raw(self, RawExpression expr):
        pass

    def visit_field(self, expr):
        pass

    def visit_path(self, PathExpression expr):
        for a in expr._path_:
            self.visit(a)


cdef class EntityReplacer(ReplacerBase):
    def __cinit__(self, EntityType what, EntityType to):
        self.what = what
        self.to = to

    def visit_field(self, Field expr):
        cdef EntityType fent = expr._entity_
        if fent is self.what:
            return getattr(self.to, expr._key_)
        else:
            return expr

    def visit_relation(self, Relation relation):
        cdef Relation clone

        if relation._entity_ is self.what:
            clone = relation.clone()
            if not clone.bind(self.to):
                raise RuntimeError("...")
            return clone
        else:
            return relation


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


cdef class FieldExtractor(Walk):
    def __cinit__(self, EntityType entity):
        self.entity = entity
        self.fields = []

    def visit_field(self, Field field):
        if field._entity_ is self.entity:
            self.fields.append(field)


cdef class FieldReplacer(ReplacerBase):
    def __cinit__(self, tuple fields, tuple values):
        self.fields = fields
        self.values = values

    def visit_field(self, Field expr):
        cdef Field repl

        for i, repl in enumerate(self.fields):
            if field_eq(expr, repl):
                return coerce_expression(self.values[i])

        return expr
