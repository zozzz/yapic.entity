from ._expression cimport Expression, Visitor, BinaryExpression, UnaryExpression, OrderExpression, AliasExpression, CastExpression, CallExpression, RawExpression, PathExpression, coerce_expression, ExpressionPlaceholder, OverExpression, ParamExpression
from ._entity cimport EntityType, EntityAttribute
from ._field cimport Field, field_eq
from ._relation cimport Relation, ManyToMany, RelationImpl, RelatedAttribute


cdef class ReplacerBase(Visitor):
    def visit_binary(self, BinaryExpression expr):
        return expr.op(self.visit(expr.left), self.visit(expr.right))

    def visit_unary(self, UnaryExpression expr):
        return expr.op(self.visit(expr.expr))

    def visit_order(self, OrderExpression expr):
        if expr.is_asc:
            return self.visit(expr.expr).asc()
        else:
            return self.visit(expr.expr).desc()

    def visit_alias(self, AliasExpression expr):
        return self.visit(expr.expr).alias(expr.value)

    def visit_cast(self, CastExpression expr):
        return self.visit(expr.expr).cast(expr.type)

    def visit_call(self, CallExpression expr):
        return self.visit(expr.callable)(*self._visit_iterable(expr.args))

    def visit_raw(self, RawExpression expr):
        cdef list exprs = []
        for e in expr.exprs:
            if isinstance(e, Expression):
                exprs.append(self.visit(e))
            else:
                exprs.append(e)
        return RawExpression(*exprs)

    def visit_param(self, ParamExpression expr):
        return expr

    def visit_field(self, expr):
        return expr

    def visit_over(self, OverExpression expr):
        over = self.visit(expr.expr).over()
        for order in expr._order:
            over.order(self.visit(order))
        for partition in expr._partition:
            over.partition(self.visit(partition))
        return over

    def visit_const(self, expr):
        return expr

    def visit_path(self, PathExpression expr):
        return PathExpression(self._visit_iterable(expr._path_))

    def visit_placeholder(self, placeholder):
        return placeholder


cdef class Walk(Visitor):
    def visit_binary(self, BinaryExpression expr):
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_unary(self, UnaryExpression expr):
        self.visit(expr.expr)

    def visit_order(self, OrderExpression expr):
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

    def visit_over(self, OverExpression expr):
        over = self.visit(expr.expr)
        for order in over._order:
            self.visit(order)
        for partition in over._partition:
            self.visit(partition)

    def visit_const(self, expr):
        pass

    def visit_path(self, PathExpression expr):
        for a in expr._path_:
            self.visit(a)


cdef class EntityReplacer(ReplacerBase):
    def __cinit__(self, EntityType what, EntityType to):
        self.what = what
        self.to = to

    def visit_field(self, Field expr):
        if expr.get_entity() is self.what:
            return getattr(self.to, expr._key_)
        else:
            return expr

    def visit_relation(self, Relation relation):
        raise RuntimeError(f"Not implemented entity replace in relation: {relation}")
        # cdef Relation result
        # if relation.get_entity() is self.what:
        #     result = relation._rebind(self.to)
        #     (<RelationImpl>result._impl_).set_joined_alias((<RelationImpl>relation._impl_).get_joined_alias())
        #     if isinstance(relation._impl_, ManyToMany):
        #         (<ManyToMany>result._impl_).set_across_alias((<ManyToMany>relation._impl_).get_across_alias())

        #     print(relation, ">>>", result)
        #     return result
        # else:
        #     return relation

    def visit_related_attribute(self, RelatedAttribute expr):
        if expr.get_entity() is self.what:
            return getattr(self.to, expr._key_)
        else:
            return expr


cdef class PlaceholderReplacer(ReplacerBase):
    def __cinit__(self, dict placeholder):
        self.placeholder = placeholder

    def visit_placeholder(self, ExpressionPlaceholder placeholder):
        try:
            origin = self.placeholder[placeholder.name]
        except KeyError:
            raise ValueError(f"Missing placeholder replacement for: {placeholder.name}")
        return placeholder.eval(origin)

    def __default__(self, object expr):
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

            if attr.get_entity() is self.where_t:
                self.where_o.__state__.set_value(attr, value)
            else:
                return expr.op(left, right)
        else:
            return expr.op(left, right)

    def visit_field(self, Field expr):
        cdef EntityType ent = expr.get_entity()
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
        if field.get_entity() is self.entity:
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

    # TODO:
    # def visit_relation(self, Relation relation):
    #     cdef Relation clone = relation._rebind(relation.get_entity())

    #     if isinstance(clone._impl_, ManyToMany):
    #         clone._impl_.join_expr = self.visit(clone._impl_.join_expr)
    #         clone._impl_.across_join_expr = self.visit(clone._impl_.across_join_expr)
    #     else:
    #         clone._impl_.join_expr = self.visit(clone._impl_.join_expr)
