import operator

from yapic.entity._entity cimport EntityType, EntityAttribute, get_alias_target
from yapic.entity._query cimport Query
from yapic.entity._expression cimport (
    Expression,
    Visitor,
    BinaryExpression,
    UnaryExpression,
    ConstExpression,
    CastExpression,
    DirectionExpression,
    AliasExpression,
    CallExpression,
    RawExpression,
    PathExpression)
from yapic.entity._expression import and_
from yapic.entity._field cimport Field, PrimaryKey
from yapic.entity._field_impl cimport JsonImpl, CompositeImpl
from yapic.entity._relation cimport Relation

from .._query_compiler cimport QueryCompiler
from ._dialect cimport PostgreDialect


cdef class PostgreQueryCompiler(QueryCompiler):
    cpdef init_subquery(self, PostgreQueryCompiler parent):
        self.parent = parent

    cpdef compile_select(self, Query query):
        query = query.finalize()
        self.parts = ["SELECT"]
        self.table_alias = self.parent.table_alias if self.parent else {}
        self.params = self.parent.params if self.parent else []

        self.collect_select = False
        self.select = []

        from_ = self.visit_from_clause(query.from_clause)
        if query.joins:
            join = self.visit_joins(query.joins)
        else:
            join = None

        if query.prefixes:
            self.parts.append(" ".join(query.prefixes))

        if query.columns:
            self.parts.append(", ".join(self.visit_columns(query.columns)))

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

    def visit_columns(self, columns):
        self.collect_select = True
        result = []

        for col in columns:
            if isinstance(col, EntityType):
                self.select.append(col)

                tbl = self.table_alias[col][1]
                for field in (<EntityType>col).__fields__:
                    result.append(f"{tbl}.{self.dialect.quote_ident(field._name_)}")
            else:
                result.append(self.visit(col))

        self.collect_select = False
        return result

    def visit_field(self, field):
        try:
            tbl = self.table_alias[field._entity_][1]
        except KeyError:
            raise RuntimeError("Entity is missing from query: %r" % field._entity_)
            # tbl = self.dialect.table_qname(field._entity_)
        if self.collect_select is True:
            self.select.append(field)

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

    # def visit_relation_attribute(self, RelationAttribute expr):
    #     return self.visit(expr.attr)

    def visit_path(self, PathExpression expr):
        cdef list path = [expr._primary_]  + expr._path_
        cdef int i = 0
        cdef int l = len(path)
        cdef str compiled = None
        cdef list attrs = []
        cdef str state = None

        while i < l:
            item = (<list>path)[i]

            if state == "relation":
                if compiled:
                    raise NotImplementedError()
                else:
                    compiled = self.visit(item)
            elif state == "json":
                if isinstance(item, Field):
                    attrs.append((<Field>item)._name_)
                elif isinstance(item, str):
                    attrs.append(item)
                elif isinstance(item, int):
                    attrs.append(str(item))
                else:
                    raise ValueError("Invalid json path entry: %r" % item)
            elif state == "composite":
                if isinstance(item, Field):
                    attrs.append((<Field>item)._name_)
                else:
                    raise ValueError("Invalid composite path entry: %r" % item)

            if isinstance(item, Relation):
                if compiled:
                    raise NotImplementedError()
                state = "relation"
            elif isinstance(item, Field):
                new_state = state
                if isinstance((<Field>item)._impl_, JsonImpl):
                    new_state = "json"
                elif isinstance((<Field>item)._impl_, CompositeImpl):
                    if state != "json":
                        new_state = "composite"

                if not new_state:
                    raise ValueError("Unexpected field impl: %r", (<Field>item)._impl_)

                if new_state != state:
                    compiled = path_expr(self.dialect, state, compiled, attrs)
                    attrs = []
                    state = new_state
                    if not compiled:
                        compiled = self.visit(item)

            i += 1

        return path_expr(self.dialect, state, compiled, attrs)

    def visit_call(self, CallExpression expr):
        args = []
        for a in expr.args:
            args.append(self.visit(a))
        return f"{self.visit(expr.callable)}({', '.join(args)})"

    def visit_raw(self, RawExpression expr):
        return expr.expr

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

    cpdef compile_insert(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        if not values:
            return None

        cdef list inserts = []

        if inline_values:
            for v in values:
                inserts.append(self.dialect.quote_value(v))
        else:
            for i in range(1, len(names) + 1):
                inserts.append(f"${i}")

        return "".join(("INSERT INTO ", self.dialect.table_qname(entity),
            "(", ", ".join(names), ") VALUES (", ", ".join(inserts), ")"))

    cpdef compile_insert_or_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        if not values:
            return None

        cdef EntityAttribute attr
        cdef list updates = []
        cdef list inserts = []
        cdef list pk_names = [self.dialect.quote_ident(attr._name_) for attr in entity.__pk__]

        if inline_values:
            for i, name in enumerate(names):
                attr = <EntityAttribute>attrs[i]
                val = self.dialect.quote_value(values[i])
                inserts.append(val)
                if not attr.get_ext(PrimaryKey):
                    updates.append(f"{name}={val}")
        else:
            for i, name in enumerate(names):
                attr = <EntityAttribute>attrs[i]
                inserts.append(f"${i+1}")
                if not attr.get_ext(PrimaryKey):
                    updates.append(f"{name}=${i+1}")

        q = ["INSERT INTO ", self.dialect.table_qname(entity),
            " (", ", ".join(names), ") VALUES (", ", ".join(inserts), ")",
            " ON CONFLICT "]

        if pk_names:
            q.extend(("(", ", ".join(pk_names), ") "))

        if updates:
            q.extend(("DO UPDATE SET ", ", ".join(updates)))
        else:
            q.append("DO NOTHING")
        return "".join(q)

    cpdef compile_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        if not values:
            return None

        cdef EntityAttribute attr
        cdef list updates = []
        cdef list where = []

        if inline_values:
            for i, name in enumerate(names):
                attr = <EntityAttribute>attrs[i]
                val = self.dialect.quote_value(values[i])

                if attr.get_ext(PrimaryKey):
                    where.append(f"{name}={val}")
                else:
                    updates.append(f"{name}={val}")
        else:
            for i, name in enumerate(names):
                attr = <EntityAttribute>attrs[i]

                if attr.get_ext(PrimaryKey):
                    where.append(f"{name}=${i+1}")
                else:
                    updates.append(f"{name}=${i+1}")

        if not updates:
            return None

        if not where:
            raise RuntimeError("TODO: ...")

        return "".join(("UPDATE ", self.dialect.table_qname(entity), " SET ",
            ", ".join(updates), " WHERE ", " AND ".join(where)))

    cpdef compile_delete(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        """

        Returns:
            Returns with a 2 element tuple:
                1. element is query string
                2. element is params
        """
        cdef list where = []
        cdef list params = []

        if inline_values:
            for i, attr in enumerate(attrs):
                if attr.get_ext(PrimaryKey):
                    where.append(f"{names[i]}={self.dialect.quote_value(values[i])}")
        else:
            for i, attr in enumerate(attrs):
                if attr.get_ext(PrimaryKey):
                    where.append(f"{names[i]}=${i+1}")
                    params.append(values[i])

        if not where:
            if not values:
                return (None, None)

            if inline_values:
                for i, attr in enumerate(attrs):
                    where.append(f"{names[i]}={self.dialect.quote_value(values[i])}")
            else:
                for i, attr in enumerate(attrs):
                    where.append(f"{names[i]}=${i+1}")
                    params.append(values[i])

        return "".join(("DELETE FROM ", self.dialect.table_qname(entity),
             " WHERE ", " AND ".join(where))), params


cdef visit_list(PostgreQueryCompiler qc, list items):
    cdef list res = []
    for x in items:
        res.append(qc.visit(x))
    return res


cdef compile_binary(PostgreQueryCompiler qc, BinaryExpression expr, str op):
    return f"{qc.visit(expr.left)} {op} {qc.visit(expr.right)}"


cdef str path_expr(object d, str type, str base, list path):
    if path:
        if type == "json":
            return f"jsonb_extract_path({base}, {', '.join(map(d.quote_value, path))})"
        elif type == "composite":
            return f"{base}.{'.'.join(map(d.quote_ident, path))}"
    return base


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
