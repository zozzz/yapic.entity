import operator
# from functools import cmp_to_key

from yapic.entity._entity cimport EntityType, EntityAttribute, get_alias_target
from yapic.entity._expression cimport (
    Expression,
    Visitor,
    BinaryExpression,
    UnaryExpression,
    ConstExpression,
    CastExpression,
    OrderExpression,
    AliasExpression,
    CallExpression,
    RawExpression,
    PathExpression,
    ColumnRefExpression,
    OverExpression)
from yapic.entity._expression import and_
from yapic.entity._field cimport Field, PrimaryKey
from yapic.entity._field_impl cimport JsonImpl, CompositeImpl, ArrayImpl
from yapic.entity._relation cimport Relation, RelatedAttribute
from yapic.entity._virtual_attr cimport VirtualAttribute

from .._query cimport Query, QueryCompiler
from ._dialect cimport PostgreDialect


cdef class PostgreQueryCompiler(QueryCompiler):
    cpdef init_subquery(self, PostgreQueryCompiler parent):
        self.parent = parent

    cpdef compile_select(self, Query query):
        query, self.rcos_list = query.finalize(self)
        # print(query)
        self.query = query
        self.parts = ["SELECT"]
        self.table_alias = {}
        self.params = self.parent.params if self.parent else []
        self.inline_values = False

        from_ = self.visit_select_from(query._select_from)
        if query._joins:
            join = self.visit_joins(query._joins)
        else:
            join = None

        if query._prefix:
            self.parts.append(" ".join(query._prefix))

        if query._as_row:
            self.skip_alias += 1
            columns = ", ".join(self.visit_columns(query._columns))
            self.parts.append(f"ROW({columns})")
            self.skip_alias -= 1
        elif query._as_json:
            raise NotImplementedError()
        else:
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
            self.parts.append(", ".join(self._visit_iterable(query._group)))

        if query._having:
            self.parts.append("HAVING")
            self.parts.append(self.visit(and_(*query._having)))

        # TODO: window

        if query._order:
            self.parts.append("ORDER BY")
            self.parts.append(", ".join(self._visit_iterable(query._order)))

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
                qname, alias = self._get_entity_alias(<EntityType>expr)
                result.append(f"{qname} {alias}")
            else:
                result.append(self.visit(expr))

        return ",".join(result)

    def visit_joins(self, dict joins):
        result = []

        for joined, condition, type in joins.values():
            qname, alias = self._get_entity_alias(joined)
            result.append(f"{type + ' ' if type else ''}JOIN {qname} {alias} ON {self.visit(condition)}")

        return " ".join(result)

    def visit_columns(self, columns):
        result = []

        for col in columns:
            if isinstance(col, EntityType):
                for field in (<EntityType>col).__fields__:
                    result.append(self.visit(field))
            else:
                result.append(self.visit(col))

        return result

    def visit_field(self, Field field):
        try:
            tbl = self._get_entity_alias(field.get_entity())[1]
        except KeyError:
            # print("MISSING", field, field.get_entity(), get_alias_target(field.get_entity()), hash(field.get_entity()))
            raise RuntimeError("Field entity is not found in query: %r" % field)
            # tbl = self.dialect.table_qname(field.get_entity())

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

    def visit_binary_and(self, BinaryExpression expr):
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

    def visit_binary_or(self, BinaryExpression expr):
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

        if isinstance(left, Field) and isinstance((<Field>left)._impl_, ArrayImpl):
            result = f"{self.visit(right)}=ANY({self.visit(left)})"
            if expr.negated:
                return f"NOT({result})"
            else:
                return result

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

        if isinstance(e, (BinaryExpression, PathExpression, VirtualAttribute)):
            return f"({self.visit(e)})::{t}"
        else:
            return f"{self.visit(e)}::{t}"

    def visit_order(self, OrderExpression expr):
        e = expr.expr
        return f"{self.visit(e)} {'ASC' if expr.is_asc else 'DESC'}"

    def visit_alias(self, expr):
        if self.skip_alias > 0:
            return self.visit((<AliasExpression>expr).expr)
        else:
            return f"{self.visit((<AliasExpression>expr).expr)} as {self.dialect.quote_ident((<AliasExpression>expr).value)}"

    def visit_column_ref(self, ColumnRefExpression expr):
        return str(expr.index + 1)

    def visit_query(self, expr):
        cdef PostgreQueryCompiler qc = self.dialect.create_query_compiler()
        qc.init_subquery(self)
        sql, params = qc.compile_select(expr)
        return f"({sql})"

    def visit_path(self, PathExpression expr):
        cdef str compiled = None
        cdef list attrs = []
        cdef str state = None

        for item in expr._path_:
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
            elif state == "array":
                if isinstance(item, int):
                    attrs.append(f"[{item}]")
                else:
                    raise ValueError("Invalid path entry: %r" % item)

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
                elif isinstance((<Field>item)._impl_, ArrayImpl):
                    new_state = "array"

                if not new_state:
                    raise ValueError("Unexpected field impl: %r", (<Field>item)._impl_)

                if new_state != state:
                    compiled = path_expr(self.dialect, state, compiled, attrs)
                    attrs = []
                    state = new_state
                    if not compiled:
                        compiled = self.visit(item)

        return path_expr(self.dialect, state, compiled, attrs)

    def visit_related_attribute(self, RelatedAttribute expr):
        return self.visit(expr.__rpath__)

    def visit_call(self, CallExpression expr):
        cdef tuple args = self._visit_iterable(expr.args)
        return f"{self.visit(expr.callable)}({', '.join(args)})"

    def visit_over(self, OverExpression expr):
        args = []
        if expr._partition:
            args.append(f"PARTITION BY {', '.join(self._visit_iterable(expr._partition))}")
        if expr._order:
            args.append(f"ORDER BY {', '.join(self._visit_iterable(expr._order))}")
        return f"{self.visit(expr.expr)} OVER({' '.join(args)})"

    def visit_raw(self, RawExpression expr):
        return expr.expr

    def _get_entity_alias(self, EntityType ent):
        try:
            return self.table_alias[ent]
        except KeyError:
            alias = self.query.get_expr_alias(ent)
            original = get_alias_target(ent)
            self.table_alias[ent] = (self.dialect.table_qname(original), self.dialect.quote_ident(alias))
            return self.table_alias[ent]

        # try:
        #     return self.table_alias[ent]
        # except KeyError:
        #     aliased = get_alias_target(ent)
        #     if aliased is ent:
        #         alias = (self.dialect.table_qname(ent), self.dialect.quote_ident(f"t{len(self.table_alias)}"))
        #     else:
        #         aname = ent.__name__ if ent.__name__ else f"t{len(self.table_alias)}"
        #         alias = (self.dialect.table_qname(aliased), self.dialect.quote_ident(aname))
        #     self.table_alias[ent] = alias
        #     if aliased not in self.table_alias:
        #         self.table_alias[aliased] = alias
        #     return alias

    cpdef compile_insert(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        if not values:
            return (f"INSERT INTO {self.dialect.table_qname(get_alias_target(entity))} DEFAULT VALUES", [])

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

        return "".join(("INSERT INTO ", self.dialect.table_qname(get_alias_target(entity)),
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


        q = ["INSERT INTO ", self.dialect.table_qname(get_alias_target(entity)),
            " (", ", ".join(names), ") VALUES (", ", ".join(inserts), ")",
            " ON CONFLICT "]

        if pk_names:
            q.extend(("(", ", ".join(pk_names), ") "))

        if updates:
            q.extend(("DO UPDATE SET ", ", ".join(updates)))
        else:
            q.append("DO NOTHING")

        return "".join(q), self.params

    cpdef compile_update(self, EntityType entity, list attrs, list names, list values, list where, bint inline_values=False):
        if not values:
            return (None, None)

        self.params = []
        self.inline_values = inline_values

        cdef list updates = []
        cdef list where_clause = []
        cdef int idx

        if inline_values:
            for i, name in enumerate(names):
                v = values[i]

                if isinstance(v, Expression):
                    v = (<Expression>v).visit(self)
                else:
                    v = self.dialect.quote_value(v)

                updates.append(f"{name}={v}")

            for k, v in where:
                where_clause.append(f"{k}={self.dialect.quote_value(v)}")
        else:
            idx = 1
            for i, name in enumerate(names):
                v = values[i]

                if isinstance(v, Expression):
                    v = (<Expression>v).visit(self)
                    updates.append(f"{name}={v}")
                else:
                    self.params.append(v)
                    updates.append(f"{name}=${len(self.params)}")

            for k, v in where:
                if isinstance(v, Expression):
                    v = (<Expression>v).visit(self)
                    where_clause.append(f"{k}={v}")
                else:
                    self.params.append(v)
                    where_clause.append(f"{k}=${len(self.params)}")

        if not updates:
            return (None, None)

        if not where_clause:
            raise RuntimeError("Missing where clause")

        return "".join(("UPDATE ", self.dialect.table_qname(get_alias_target(entity)), " SET ",
            ", ".join(updates), " WHERE ", " AND ".join(where_clause))), self.params

    cpdef compile_delete(self, EntityType entity, list attrs, list names, list values, list where, bint inline_values=False):
        """

        Returns:
            Returns with a 2 element tuple:
                1. element is query string
                2. element is params
        """
        cdef list where_clause = []
        cdef list params = []

        if inline_values:
            for k, v in where:
                where_clause.append(f"{k}={self.dialect.quote_value(v)}")
        else:
            for k, v in where:
                params.append(v)
                where_clause.append(f"{k}=${len(params)}")

        if not where_clause:
            if not values:
                return (None, None)

            if inline_values:
                for i, attr in enumerate(attrs):
                    where_clause.append(f"{names[i]}={self.dialect.quote_value(values[i])}")
            else:
                for i, attr in enumerate(attrs):
                    where_clause.append(f"{names[i]}=${i+1}")
                    params.append(values[i])

        return "".join(("DELETE FROM ", self.dialect.table_qname(get_alias_target(entity)),
             " WHERE ", " AND ".join(where_clause))), params


cdef compile_binary(PostgreQueryCompiler qc, BinaryExpression expr, str op):
    return f"{qc.visit(expr.left)} {op} {qc.visit(expr.right)}"


cdef str path_expr(object d, str type, str base, list path):
    if path:
        if type == "json":
            return f"jsonb_extract_path({base}, {', '.join(path)})"
        elif type == "composite":
            return f"({base}){''.join(path)}"
        elif type == "array":
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


# @cmp_to_key
# def order_joins_by_deps(list a, list b):
#     print("CMP", a[1], b[0], "|||", b[1], a[0])
#     if a[1] is b[0]:
#         return 1
#     elif b[1] is a[0]:
#         return -1
#     else:
#         return 0
