import operator

from yapic.entity._entity cimport EntityType, get_alias_target
from yapic.entity._query cimport Query
from yapic.entity._expression cimport BinaryExpression, UnaryExpression, ConstExpression, CastExpression, DirectionExpression, AliasExpression
from yapic.entity._expression import and_
from yapic.entity._relation cimport RelationAttribute

from .._query_compiler cimport QueryCompiler


cdef class PostgreQueryCompiler(QueryCompiler):
    cpdef init_subquery(self, PostgreQueryCompiler parent):
        self.parent = parent

    cpdef compile_select(self, Query query):
        query = query.finalize()
        self.parts = ["SELECT"]
        self.table_alias = self.parent.table_alias if self.parent else {}
        self.params = self.parent.params if self.parent else []

        from_ = self.visit_from_clause(query.from_clause)
        if query.joins:
            join = self.visit_joins(query.joins)
        else:
            join = None

        if query.prefixes:
            self.parts.append(" ".join(query.prefixes))

        if query.columns:
            self.parts.append(", ".join(visit_list(self, query.columns)))
        else:
            self.parts.append("*")

        self.parts.append("FROM")
        self.parts.append(from_)
        if join:
            self.parts.append(join)

        if query.where_clause:
            self.parts.append("WHERE")
            self.parts.append(self.visit(and_(*query.where_clause)))

        if query.groups:
            self.parts.append("GROUP BY")
            self.parts.append(", ".join(visit_list(self, query.groups)))

        if query.havings:
            self.parts.append("HAVING")
            self.parts.append(self.visit(and_(*query.havings)))

        # TODO: window

        if query.orders:
            self.parts.append("ORDER BY")
            self.parts.append(", ".join(visit_list(self, query.orders)))

        if query.range:
            if query.range.start:
                self.parts.append(f"OFFSET {query.range.start}")
            if query.range.stop is not None:
                if query.range.stop == 1:
                    self.parts.append(f"FETCH FIRST ROW ONLY")
                else:
                    self.parts.append(f"FETCH FIRST {query.range.stop - query.range.start} ROWS ONLY")

        return " ".join(self.parts), tuple(self.params)

    def visit_from_clause(self, list from_clause):
        result = []

        for i, expr in enumerate(from_clause):
            if isinstance(expr, EntityType):
                qname, alias = self._add_entity_alias(<EntityType>expr)
                result.append(f"{qname} {alias}")
            else:
                result.append(self.visit(expr))

        return ",".join(result)

    def visit_joins(self, dict joins):
        result = []

        for ent, condition, type in joins.values():
            qname, alias = self._add_entity_alias(ent)
            result.append(f"{type} JOIN {qname} {alias} ON {self.visit(condition)}")

        return " ".join(result)


    def visit_field(self, field):
        try:
            tbl = self.table_alias[field._entity_][1]
        except KeyError:
            raise RuntimeError("Entity is missing from query: %r" % field._entity_)
            # tbl = self.dialect.table_qname(field._entity_)
        return f'{tbl}.{self.dialect.quote_ident(field._name_)}'

    def visit_binary_eq(self, expr):
        cdef BinaryExpression e = <BinaryExpression> expr
        return compile_eq(self.visit(e.left), self.visit(e.right), False)

    def visit_binary_ne(self, expr):
        cdef BinaryExpression e = <BinaryExpression> expr
        return compile_eq(self.visit(e.left), self.visit(e.right), True)

    def visit_binary_lt(self, expr): return compile_binary(self, <BinaryExpression>expr, "<")
    def visit_binary_le(self, expr): return compile_binary(self, <BinaryExpression>expr, "<=")
    def visit_binary_ge(self, expr): return compile_binary(self, <BinaryExpression>expr, ">=")
    def visit_binary_gt(self, expr): return compile_binary(self, <BinaryExpression>expr, ">")
    def visit_binary_add(self, expr): return compile_binary(self, <BinaryExpression>expr, "+")
    def visit_binary_sub(self, expr): return compile_binary(self, <BinaryExpression>expr, "-")
    def visit_binary_lshift(self, expr): return compile_binary(self, <BinaryExpression>expr, "<<")
    def visit_binary_rshift(self, expr): return compile_binary(self, <BinaryExpression>expr, ">>")
    def visit_binary_mod(self, expr): return compile_binary(self, <BinaryExpression>expr, "%")
    def visit_binary_mul(self, expr): return compile_binary(self, <BinaryExpression>expr, "*")
    def visit_binary_truediv(self, expr): return compile_binary(self, <BinaryExpression>expr, "/")
    def visit_binary_pow(self, expr): return compile_binary(self, <BinaryExpression>expr, "^")
    def visit_unary_invert(self, expr): return compile_unary(self, (<UnaryExpression>expr).expr, "NOT ")
    def visit_unary_neg(self, expr): return compile_unary(self, (<UnaryExpression>expr).expr, "-")
    def visit_unary_pos(self, expr): return compile_unary(self, (<UnaryExpression>expr).expr, "+")
    def visit_unary_abs(self, expr): return compile_unary(self, (<UnaryExpression>expr).expr, "@")

    def visit_binary_and(self, expr):
        cdef BinaryExpression e = <BinaryExpression> expr
        parts = []

        if isinstance(expr.left, BinaryExpression) and (<BinaryExpression>expr.left).op is operator.__or__:
            parts.append(f"({self.visit(expr.left)})")
        else:
            parts.append(self.visit(expr.left))

        if isinstance(expr.right, BinaryExpression) and (<BinaryExpression>expr.right).op is operator.__or__:
            parts.append(f"({self.visit(expr.right)})")
        else:
            parts.append(self.visit(expr.right))

        return " AND ".join(parts)

    def visit_binary_or(self, expr):
        cdef BinaryExpression e = <BinaryExpression> expr
        parts = []

        if isinstance(expr.left, BinaryExpression) and (<BinaryExpression>expr.left).op is operator.__and__:
            parts.append(f"({self.visit(expr.left)})")
        else:
            parts.append(self.visit(expr.left))

        if isinstance(expr.right, BinaryExpression) and (<BinaryExpression>expr.right).op is operator.__and__:
            parts.append(f"({self.visit(expr.right)})")
        else:
            parts.append(self.visit(expr.right))

        return " OR ".join(parts)

    def visit_binary_in(self, expr):
        left = (<BinaryExpression>expr).left
        right = (<BinaryExpression>expr).right

        if isinstance(right, ConstExpression):
            value = (<ConstExpression>right).value
            if isinstance(value, list) or isinstance(value, tuple):
                entries = [self.visit(x) for x in value]
            else:
                entries = [self.visit(value)]
        else:
            entries = [self.visit(right)]

        return f"{self.visit(left)} IN ({', '.join(entries)})"

    def visit_const(self, expr):
        value = (<ConstExpression>expr).value

        if value is None:
            return "NULL"
        elif value is True:
            return "TRUE"
        elif value is False:
            return "FALSE"
        # elif isinstance(value, int) or isinstance(value, float):
        #     return value

        try:
            idx = self.params.index(value)
        except ValueError:
            self.params.append(value)
            return f"${len(self.params)}"
        else:
            return f"${idx + 1}"

    def visit_cast(self, expr):
        e = (<CastExpression>expr).expr
        t = (<CastExpression>expr).to

        if isinstance(e, BinaryExpression):
            return f"({self.visit(e)})::{t}"
        else:
            return f"{self.visit(e)}::{t}"

    def visit_direction(self, expr):
        e = (<DirectionExpression>expr).expr
        return f"{self.visit(e)} {'ASC' if (<DirectionExpression>expr).is_asc else 'DESC'}"



    def visit_alias(self, expr):
        return f"{self.visit((<AliasExpression>expr).expr)} as {self.dialect.quote_ident((<AliasExpression>expr).value)}"

    def visit_query(self, expr):
        cdef PostgreQueryCompiler qc = self.dialect.create_query_compiler()
        qc.init_subquery(self)
        sql, params = qc.compile_select(expr)
        return f"({sql})"

    def visit_relation_attribute(self, RelationAttribute expr):
        return self.visit(expr.attr)

    def _add_entity_alias(self, EntityType ent):
        try:
            return self.table_alias[ent]
        except KeyError:
            aliased = get_alias_target(ent)
            if aliased is ent:
                alias = (self.dialect.table_qname(ent), self.dialect.quote_ident(f"t{len(self.table_alias)}"))
            else:
                alias = (self.dialect.table_qname(aliased), self.dialect.quote_ident(ent.__name__))
            self.table_alias[ent] = alias
            return alias


cdef visit_list(PostgreQueryCompiler qc, list items):
    cdef list res = []
    for x in items:
        res.append(qc.visit(x))
    return res


cdef compile_binary(PostgreQueryCompiler qc, BinaryExpression expr, str op):
    return f"{qc.visit(expr.left)} {op} {qc.visit(expr.right)}"


cdef compile_eq(left, right, bint neg):
    if left == "NULL" or left == "TRUE" or left == "FALSE" \
            or right == "NULL" or right == "TRUE" or right == "FALSE":
        if neg:
            return f"{left} IS NOT {right}"
        else:
            return f"{left} IS {right}"
    elif neg:
        return f"{left} != {right}"
    else:
        return f"{left} = {right}"

cdef compile_unary(PostgreQueryCompiler qc, expr, str op):
    if isinstance(expr, BinaryExpression):
        return f"{op}({qc.visit(expr)})"
    else:
        return f"{op}{qc.visit(expr)}"
