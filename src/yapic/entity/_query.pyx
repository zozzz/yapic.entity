from contextlib import contextmanager

from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field
from yapic.entity._expression cimport (Expression, AliasExpression, DirectionExpression, Visitor, BinaryExpression,
    UnaryExpression, CastExpression, CallExpression, RawExpression, PathExpression)
from yapic.entity._expression import and_
from yapic.entity._relation cimport Relation, RelationImpl, ManyToMany, RelatedField, determine_join_expr
from yapic.entity._error cimport JoinError


cdef class Query(Expression):
    def __cinit__(self):
        self.entities = {}

    cpdef visit(self, Visitor visitor):
        return visitor.visit_query(self)

    def select_from(self, from_):
        if self.from_clause is None:
            self.from_clause = []

        if from_ not in self.from_clause:
            self.from_clause.append(from_)

        if isinstance(from_, EntityType):
            self._add_entity(<EntityType>from_)

        return self

    def column(self, *columns):
        if self.columns is None:
            self.columns = []

        for col in columns:

            # if isinstance(col, EntityType):
            #     # if self.entity_columns is None:
            #     #     self.entity_columns = []
            #     # self.entity_columns.append((len(self.columns), col))
            #     self.columns.append()
            # elif isinstance(col, Field) or isinstance(col, AliasExpression):
            #     self.columns.append(col)
            # elif isinstance(col, RawExpression):
            #     self.columns.append(col)
            # else:
            #     raise ValueError("Invalid value for column: %r" % col)

            if isinstance(col, EntityType) \
                    or isinstance(col, Field) \
                    or isinstance(col, AliasExpression) \
                    or isinstance(col, RawExpression):
                self.columns.append(col)
            else:
                raise ValueError("Invalid value for column: %r" % col)

        return self

    def where(self, *expr, **eq):
        if self.where_clause is None:
            self.where_clause = []

        self.where_clause.append(and_(*expr))
        if eq:
            raise NotImplementedError()
        return self


    def order(self, *expr):
        if self.orders is None:
            self.orders = []

        for item in expr:
            if isinstance(item, DirectionExpression) or isinstance(item, RawExpression):
                self.orders.append(item)
            elif isinstance(item, Expression):
                self.orders.append((<Expression>item).asc())
            else:
                raise ValueError("Invalid value for order: %r" % item)

        return self

    def group(self, *expr):
        if self.groups is None:
            self.groups = []

        for item in expr:
            if not isinstance(item, Expression):
                raise ValueError("Invalid value for group: %r" % item)
            else:
                self.groups.append(item)

        return self

    def having(self, *expr, **eq):
        if self.havings is None:
            self.havings = []

        self.havings.append(and_(*expr))
        if eq:
            raise NotImplementedError()
        return self

    def distinct(self, *expr):
        if self.distincts is None:
            self.distincts = []

        if expr:
            try:
                self.prefixes.remove("DISTINCT")
            except:
                pass
        else:
            self.prefix("DISTINCT")
        return self

    def prefix(self, *prefix):
        if self.prefixes is None:
            self.prefixes = []

        for p in prefix:
            if p not in self.prefixes:
                self.prefixes.append(p)

        return self

    def suffix(self, *suffix):
        if self.suffixes is None:
            self.suffixes = []

        for s in suffix:
            if s not in self.suffixes:
                self.suffixes.append(s)

        return self

    def join(self, what, condition = None, type = "INNER"):
        cdef RelationImpl impl

        if self.joins is None:
            self.joins = {}

        if what in self.entities:
            return self

        if isinstance(what, EntityType):
            if condition is None:
                condition = determine_join(self, <EntityType>what)
            self.joins[what] = (what, condition, type)
            self._add_entity(what)
        elif isinstance(what, Relation):
            impl = (<Relation>what)._impl_

            if isinstance(impl, ManyToMany):
                cross_condition = (<ManyToMany>impl).across_join_expr
                cross_what = (<ManyToMany>impl).across

                if cross_what not in self.joins:
                    self.joins[cross_what] = (cross_what, cross_condition, "INNER")

                condition = impl.join_expr
                what = impl.joined
            else:
                condition = impl.join_expr
                what = impl.joined

            self.joins[what] = (what, condition, type)
            self._add_entity(what)

        return self

    def limit(self, int count):
        if self.range is None:
            self.range = slice(0, count)
        else:
            self.range = slice(self.range.start, self.range.start + count)
        return self

    def offset(self, int offset):
        if self.range is None:
            self.range = slice(offset, None)
        else:
            if self.range.stop:
                count = self.range.stop - self.range.start
                stop = offset + count
            else:
                stop = None

            self.range = slice(offset, stop)
        return self

    cpdef Query clone(self):
        cdef Query q = type(self)()

        if self.from_clause: q.from_clause = list(self.from_clause)
        if self.columns: q.columns = list(self.columns)
        # if self.entity_columns: q.entity_columns = list(self.entity_columns)
        if self.where_clause: q.where_clause = list(self.where_clause)
        if self.orders: q.orders = list(self.orders)
        if self.groups: q.groups = list(self.groups)
        if self.havings: q.havings = list(self.havings)
        if self.distincts: q.distincts = list(self.distincts)
        if self.prefixes: q.prefixes = list(self.prefixes)
        if self.suffixes: q.suffixes = list(self.suffixes)
        if self.joins: q.joins = dict(self.joins)
        if self.range: q.range = slice(self.range.start, self.range.stop, self.range.step)
        if self.entities: q.entities = dict(self.entities)

        return q

    cdef Query finalize(self):
        cdef Query res = self.clone()
        cdef QueryFinalizer qf = QueryFinalizer(res)

        qf.auto_join()

        return res

    cdef _add_entity(self, EntityType ent):
        if ent not in self.entities:
            self.entities[ent] = f"t{len(self.entities)}"


cdef class QueryFinalizer(Visitor):
    def __cinit__(self, Query q):
        self.q = q

    def visit_binary(self, BinaryExpression expr):
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_unary(self, UnaryExpression expr):
        self.visit(expr.expr)

    def visit_cast(self, CastExpression expr):
        self.visit(expr.expr)

    def visit_direction(self, DirectionExpression expr):
        self.visit(expr.expr)

    def visit_call(self, CallExpression expr):
        self.visit(expr.callable)
        for a in expr.args:
            self.visit(a)

    def visit_raw(self, expr):
        pass

    def visit_field(self, expr):
        pass

    def visit_const(self, expr):
        pass

    def visit_query(self, expr):
        # TODO: ...
        pass

    def visit_alias(self, AliasExpression expr):
        self.visit(expr.expr)

    # def visit_entity_alias(self, EntityAliasExpression expr):
    #     pass

# cdef class RelationAttribute(Expression):
#     cdef Relation relation
#     cdef EntityAttribute attr

    # def visit_relation_attribute(self, RelationAttribute expr):
    #     self.q.join(expr.relation)

    def visit_path(self, PathExpression expr):
        if isinstance(expr._primary_, Relation):
            self.q.join(expr._primary_)

        return PathExpression(expr._primary_, list(expr._path_))

    def auto_join(self, *expr_list):
        if self.q.columns: self._visit_list(self.q.columns)
        if self.q.where_clause: self._visit_list(self.q.where_clause)
        if self.q.orders: self._visit_list(self.q.orders)
        if self.q.groups: self._visit_list(self.q.groups)
        if self.q.havings: self._visit_list(self.q.havings)
        if self.q.distincts: self._visit_list(self.q.distincts)

        if not self.q.columns:
            self.q.columns = list(self.q.from_clause)

    def _visit_list(self, expr_list):
        for expr in expr_list:
            if isinstance(expr, EntityType):
                self.q.join(expr)
            else:
                self.visit(expr)

    @contextmanager
    def _replace_visitor(self, name, v):
        original = getattr(self, name)
        try:
            setattr(self, name, v)
            yield self
        finally:
            setattr(self, name, original)


cdef inline determine_join(Query q, EntityType joined):
    cdef EntityType ent
    condition = None

    for ent in q.entities:
        try:
            condition = determine_join_expr(ent, joined)
        except JoinError:
            try:
                condition = determine_join_expr(joined, ent)
            except JoinError:
                continue
            else:
                return condition
        else:
            return condition

    raise JoinError("Can't found suitable join condition between %r <-> %r" % (q, joined))

