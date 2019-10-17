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
        query, self.rcos_list = query.finalize()
        self.parts = ["SELECT"]
        self.table_alias = self.parent.table_alias if self.parent else {}
        self.params = self.parent.params if self.parent else []
        self.inline_values = False

        from_ = self.visit_select_from(query._select_from)
        if query._joins:
            join = self.visit_joins(query._joins)
        else:
            join = None

        if query._prefix:
            self.parts.append(" ".join(query._prefix))

        self.parts.append(", ".join(self.visit_columns(query._columns)))

        self.parts.append("FROM")
        self.parts.append(from_)
        if join:
            self.parts.append(join)

        if query._where:
            self.parts.append("WHERE")
            self.parts.append(self.visit(and_(*query._where)))

        if query._group:
            self.parts.append("GROUP BY")
            self.parts.append(", ".join(visit_list(self, query._group)))

        if query._having:
            self.parts.append("HAVING")
            self.parts.append(self.visit(and_(*query._having)))

        # TODO: window

        if query._order:
            self.parts.append("ORDER BY")
            self.parts.append(", ".join(visit_list(self, query._order)))

        if query._range:
            if query._range.start:
                self.parts.append(f"OFFSET {query._range.start}")
            if query._range.stop is not None:
                if query._range.stop == 1:
                    self.parts.append(f"FETCH FIRST ROW ONLY")
                else:
                    self.parts.append(f"FETCH FIRST {query._range.stop - query._range.start} ROWS ONLY")

        return " ".join(self.parts), tuple(self.params)

    def visit_select_from(self, list select_from):
        result = []

        for i, expr in enumerate(select_from):
            if isinstance(expr, EntityType):
                qname, alias = self._add_entity_alias(<EntityType>expr)
                result.append(f"{qname} {alias}")
            else:
                result.append(self.visit(expr))

        return ",".join(result)

    def visit_joins(self, dict joins):
        result = []

        for joined, condition, type in joins.values():
            qname, alias = self._add_entity_alias(joined)
            result.append(f"{type + ' ' if type else ''}JOIN {qname} {alias} ON {self.visit(condition)}")

        return " ".join(result)

    def visit_columns(self, columns):
        result = []

        for col in columns:
            if isinstance(col, EntityType):
                tbl = self.table_alias[col][1]
                for field in (<EntityType>col).__fields__:
                    result.append(self.visit(field))
            else:
                result.append(self.visit(col))

        return result

    def visit_field(self, field):
        try:
            tbl = self.table_alias[field._entity_][1]
        except KeyError:
            # print("MISSING", field, field._entity_, get_alias_target(field._entity_), hash(field._entity_))
            raise RuntimeError("Field entity is not found in query: %r" % field)
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

    def visit_binary_in(self, BinaryExpression expr):
        left = expr.left
        right = expr.right

        if isinstance(right, ConstExpression):
            value = (<ConstExpression>right).value
            if isinstance(value, (list, tuple)):
                entries = [self.visit(x) for x in value]
            else:
                entries = [self.visit(value)]
        else:
            entries = [self.visit(right)]

        if entries:
            op = " NOT IN " if expr.negated else " IN "
            return f"{self.visit(left)}{op}({', '.join(entries)})"
        else:
            return f"FALSE"

    def visit_binary_startswith(self, BinaryExpression expr):
        left = expr.left
        right = expr.right
        op = " NOT ILIKE " if expr.negated else " ILIKE "
        return f"{self.visit(left)}{op}({self.visit(right)} || '%')"

    def visit_binary_endswith(self, BinaryExpression expr):
        left = expr.left
        right = expr.right
        op = " NOT ILIKE " if expr.negated else " ILIKE "
        return f"{self.visit(left)}{op}('%' || {self.visit(right)})"

    def visit_binary_contains(self, BinaryExpression expr):
        left = expr.left
        right = expr.right
        op = " NOT ILIKE " if expr.negated else " ILIKE "
        return f"{self.visit(left)}{op}('%' || {self.visit(right)} || '%')"

    def visit_binary_find(self, BinaryExpression expr):
        left = expr.left
        right = expr.right
        return f"POSITION(LOWER({self.visit(right)}) IN LOWER({self.visit(left)}))"

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

        if self.inline_values:
            return self.dialect.quote_value(value)
        else:
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

    def visit_path(self, PathExpression expr):
        cdef str compiled = None
        cdef list attrs = []
        cdef str state = None

        for item in (<list>expr._path_):
            if state == "relation":
                if compiled:
                    raise NotImplementedError()
                elif isinstance(item, Relation):
                    continue
                else:
                    compiled = self.visit(item)
            elif state == "json":
                if isinstance(item, Field):
                    attrs.append(self.dialect.quote_value((<Field>item)._name_))
                elif isinstance(item, str):
                    attrs.append(self.dialect.quote_value(item))
                elif isinstance(item, int):
                    attrs.append(str(item))
                else:
                    raise ValueError("Invalid json path entry: %r" % item)
            elif state == "composite":
                if isinstance(item, Field):
                    attrs.append("." + self.dialect.quote_ident((<Field>item)._name_))
                elif isinstance(item, int):
                    attrs.append(f"[{item}]")
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
                aname = ent.__name__ if ent.__name__ else f"t{len(self.table_alias)}"
                alias = (self.dialect.table_qname(aliased), self.dialect.quote_ident(aname))
            self.table_alias[ent] = alias
            if aliased not in self.table_alias:
                self.table_alias[aliased] = alias
            return alias

    cpdef compile_insert(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        if not values:
            return (f"INSERT INTO {self.dialect.table_qname(entity)} DEFAULT VALUES", [])

        self.params = []
        self.inline_values = inline_values

        cdef list inserts = []

        if inline_values:
            for v in values:
                if isinstance(v, Expression):
                    inserts.append((<Expression>v).visit(self))
                else:
                    inserts.append(self.dialect.quote_value(v))
        else:
            for i in range(0, len(names)):
                v = values[i]
                if isinstance(v, Expression):
                    inserts.append((<Expression>v).visit(self))
                else:
                    self.params.append(v)
                    inserts.append(f"${len(self.params)}")

        return "".join(("INSERT INTO ", self.dialect.table_qname(entity),
            " (", ", ".join(names), ") VALUES (", ", ".join(inserts), ")")), self.params

    cpdef compile_insert_or_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        if not values:
            return (None, None)

        self.params = []
        self.inline_values = inline_values

        cdef EntityAttribute attr
        cdef list updates = []
        cdef list inserts = []
        cdef list pk_names = [self.dialect.quote_ident(attr._name_) for attr in entity.__pk__]
        cdef int idx

        if inline_values:
            for i, name in enumerate(names):
                v = values[i]

                if isinstance(v, Expression):
                    v = (<Expression>v).visit(self)
                else:
                    v = self.dialect.quote_value(v)

                inserts.append(v)
                if not (<EntityAttribute>attrs[i]).get_ext(PrimaryKey):
                    updates.append(f"{name}={v}")
        else:
            idx = 1
            for i, name in enumerate(names):
                v = values[i]

                if isinstance(v, Expression):
                    v = (<Expression>v).visit(self)
                    inserts.append(v)
                    if not (<EntityAttribute>attrs[i]).get_ext(PrimaryKey):
                        updates.append(f"{name}={v}")
                else:
                    self.params.append(v)
                    idx = len(self.params)
                    inserts.append(f"${idx}")
                    if not (<EntityAttribute>attrs[i]).get_ext(PrimaryKey):
                        updates.append(f"{name}=${idx}")


        q = ["INSERT INTO ", self.dialect.table_qname(entity),
            " (", ", ".join(names), ") VALUES (", ", ".join(inserts), ")",
            " ON CONFLICT "]

        if pk_names:
            q.extend(("(", ", ".join(pk_names), ") "))

        if updates:
            q.extend(("DO UPDATE SET ", ", ".join(updates)))
        else:
            q.append("DO NOTHING")

        return "".join(q), self.params

    cpdef compile_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        if not values:
            return (None, None)

        self.params = []
        self.inline_values = inline_values

        cdef list updates = []
        cdef list where = []
        cdef int idx

        if inline_values:
            for i, name in enumerate(names):
                v = values[i]

                if isinstance(v, Expression):
                    v = (<Expression>v).visit(self)
                else:
                    v = self.dialect.quote_value(v)

                if (<EntityAttribute>attrs[i]).get_ext(PrimaryKey):
                    where.append(f"{name}={v}")
                else:
                    updates.append(f"{name}={v}")
        else:
            idx = 1
            for i, name in enumerate(names):
                v = values[i]

                if isinstance(v, Expression):
                    v = (<Expression>v).visit(self)
                    if (<EntityAttribute>attrs[i]).get_ext(PrimaryKey):
                        where.append(f"{name}={v}")
                    else:
                        updates.append(f"{name}={v}")
                else:
                    self.params.append(v)
                    if (<EntityAttribute>attrs[i]).get_ext(PrimaryKey):
                        where.append(f"{name}=${len(self.params)}")
                    else:
                        updates.append(f"{name}=${len(self.params)}")

        if not updates:
            return (None, None)

        if not where:
            raise RuntimeError("TODO: ...")

        return "".join(("UPDATE ", self.dialect.table_qname(entity), " SET ",
            ", ".join(updates), " WHERE ", " AND ".join(where))), self.params

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
            return f"jsonb_extract_path({base}, {', '.join(path)})"
        elif type == "composite":
            return f"({base}){''.join(path)}"
    return base


cdef compile_eq(left, right, bint neg):
    if left == "NULL" or left == "TRUE" or left == "FALSE":
        tmp = left
        left = right
        right = tmp

    if right == "NULL" or right == "TRUE" or right == "FALSE":
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
