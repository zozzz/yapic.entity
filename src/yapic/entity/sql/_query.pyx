import operator
import cython

from yapic.entity._entity cimport EntityType, EntityAttribute, Polymorph, get_alias_target, is_entity_alias
from yapic.entity._field cimport Field, field_eq
from yapic.entity._field_impl cimport CompositeImpl
from yapic.entity._expression cimport (Expression, AliasExpression, ColumnRefExpression, OrderExpression, Visitor,
    BinaryExpression, UnaryExpression, CastExpression, CallExpression, RawExpression, PathExpression,
    ConstExpression, raw)
from yapic.entity._expression import and_
from yapic.entity._relation cimport Relation, RelationImpl, ManyToOne, ManyToMany, RelatedAttribute, determine_join_expr, Loading
from yapic.entity._error cimport JoinError
from yapic.entity._visitors cimport extract_fields, replace_fields, replace_entity, ReplacerBase
from yapic.entity._virtual_attr cimport VirtualAttribute, VirtualOrderExpression, VirtualBinaryExpression

from ._dialect cimport Dialect


cdef class Query(Expression):
    def __cinit__(self):
        self._entities = set()
        self._load = QueryLoad()
        self._parent = None
        self._allow_clone = True
        self.__expr_alias = {}
        self.__alias_c = 0
        self._pending_joins = []

    def __init__(self, from_ = None):
        if from_ is not None:
            self.select_from(from_)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_query(self)

    def select_from(self, from_):
        if self._select_from is None:
            self._select_from = []

        if from_ not in self._select_from:
            self._select_from.append(from_)

        if isinstance(from_, EntityType):
            self._entities.add(from_)

        return self

    def columns(self, *columns):
        if self._columns is None:
            self._columns = []

        for col in columns:
            if isinstance(col, EntityType) \
                    or isinstance(col, Field) \
                    or isinstance(col, AliasExpression) \
                    or isinstance(col, PathExpression) \
                    or isinstance(col, RawExpression) \
                    or isinstance(col, CallExpression) \
                    or isinstance(col, VirtualAttribute) \
                    or isinstance(col, Query):
                self._columns.append(col)
            else:
                raise ValueError("Invalid value for column: %r" % col)

        return self

    def where(self, *expr, **eq):
        if self._where is None:
            self._where = []

        self._where.append(and_(*expr))
        if eq:
            raise NotImplementedError()
        return self


    def order(self, *expr):
        if self._order is None:
            self._order = []

        for item in expr:
            if isinstance(item, (OrderExpression, RawExpression)):
                self._order.append(item)
            elif isinstance(item, Expression):
                self._order.append((<Expression>item).asc())
            else:
                raise ValueError("Invalid value for order: %r" % item)

        return self

    def group(self, *expr):
        if self._group is None:
            self._group = []

        for item in expr:
            if not isinstance(item, Expression):
                raise ValueError("Invalid value for group: %r" % item)
            else:
                self._group.append(item)

        return self

    def having(self, *expr, **eq):
        if self._having is None:
            self._having = []

        self._having.append(and_(*expr))
        if eq:
            raise NotImplementedError()
        return self

    def distinct(self, *expr):
        if self._distinct is None:
            self._distinct = []

        if expr:
            try:
                self._prefix.remove("DISTINCT")
            except:
                pass
        else:
            self.prefix("DISTINCT")
        return self

    def prefix(self, *prefix):
        if self._prefix is None:
            self._prefix = []

        for p in prefix:
            if p not in self._prefix:
                self._prefix.append(p)

        return self

    def suffix(self, *suffix):
        if self._suffix is None:
            self._suffix = []

        for s in suffix:
            if s not in self._suffix:
                self._suffix.append(s)

        return self

    def as_row(self, bint val=True):
        self._as_row = val
        if self._as_json is True and val is True:
            self._as_json = False
        return self

    def as_json(self, bint val=True):
        self._as_json = val
        if self._as_row is True and val is True:
            self._as_row = False
        return self

    def join(self, what, condition = None, type = "INNER"):
        cdef RelationImpl impl
        cdef EntityType joined

        if self._joins is None:
            self._joins = {}

        if isinstance(what, Relation):
            impl = (<Relation>what)._impl_
            condition = impl.join_expr
            joined = impl.get_joined_alias()

            if joined in self._entities:
                return self

            if (<Relation>what).get_entity() not in self._entities:
                self._pending_joins.append((what, condition, type))
                return self

            if isinstance(impl, ManyToMany):
                cross_condition = (<ManyToMany>impl).across_join_expr
                cross_what = (<ManyToMany>impl).get_across_alias()

                self._entities.add(cross_what)

                cross_what_id = id(cross_what)
                try:
                    existing = self._joins[cross_what_id]
                except KeyError:
                    self._joins[cross_what_id] = [cross_what, cross_condition, type]
                else:
                    if type.upper().startswith("INNER"):
                        existing[2] = type
                type = "INNER"

            if joined is None:
                raise RuntimeError("Relation is deferred: %r" % what)

            if condition is None:
                raise RuntimeError("Missing join expression from relation: %r" % what)

        elif isinstance(what, EntityType):
            joined = <EntityType>what

            if joined in self._entities:
                return self

            if condition is None:
                condition = determine_join(self, joined)
        else:
            raise ValueError(f"Want to join uexpected entity: {what}")

        self._entities.add(joined)

        # TODO: Nem biztos, hogy ezt így kéne...
        # aliased = get_alias_target(joined)
        # self._entities.add(aliased)

        joined_id = id(joined)
        try:
            existing = self._joins[joined_id]
        except KeyError:
            self._joins[joined_id] = [joined, condition, type]
        else:
            if type.upper().startswith("INNER"):
                existing[2] = type

        return self

    def limit(self, int count):
        if self._range is None:
            self._range = slice(0, count)
        else:
            self._range = slice(self._range.start, self._range.start + count)
        return self

    def offset(self, int offset):
        if self._range is None:
            self._range = slice(offset, None)
        else:
            if self._range.stop:
                count = self._range.stop - self._range.start
                stop = offset + count
            else:
                stop = None

            self._range = slice(offset, stop)
        return self

    def reset_columns(self):
        self._columns = None
        return self

    def reset_where(self):
        self._where = None
        return self

    def reset_order(self):
        self._order = None
        return self

    def reset_group(self):
        self._group = None
        return self

    def reset_range(self):
        self._range = None
        return self

    def reset_load(self):
        self._load = QueryLoad()
        return self

    def load(self, *load):
        self._load.add(load)
        return self

    cpdef Query clone(self):
        if not self._allow_clone:
            raise RuntimeError("Query is not cloneable")

        cdef Query q = type(self)()

        if self._select_from:   q._select_from = list(self._select_from)
        if self._columns:       q._columns = list(self._columns)
        if self._where:         q._where = list(self._where)
        if self._order:         q._order = list(self._order)
        if self._group:         q._group = list(self._group)
        if self._having:        q._having = list(self._having)
        if self._distinct:      q._distinct = list(self._distinct)
        if self._prefix:        q._prefix = list(self._prefix)
        if self._suffix:        q._suffix = list(self._suffix)
        if self._joins:         q._joins = dict(self._joins)
        if self._range:         q._range = slice(self._range.start, self._range.stop, self._range.step)
        if self._entities:      q._entities = set(self._entities)
        if self._load:          q._load = self._load.clone()
        if self._parent:        q._parent = self._parent.clone()
        if self._pending_joins: q._pending_joins = list(self._pending_joins)
        q._as_row = self._as_row
        q._as_json = self._as_json

        return q

    cdef tuple finalize(self, QueryCompiler compiler):
        if self._rcos:
            return self, self._rcos

        cdef Query res = self
        if self._allow_clone:
            res = self.clone()
            res._allow_clone = False

        QueryFinalizer(compiler, res).finalize()
        return res, res._rcos

    cdef str get_expr_alias(Query self, object expr):
        if isinstance(expr, EntityType):
            try:
                return self.__expr_alias[expr]
            except KeyError:
                entity = self._find_entity(expr, False)
                if entity is not None:
                    if is_entity_alias(entity) and entity.__name__:
                        alias = entity.__name__
                    else:
                        alias = self._get_next_alias()

                    self.__expr_alias[expr] = alias
                    return alias
                elif self._parent is not None:
                    return self._parent.get_expr_alias(expr)
                else:
                    raise ValueError(f"Can't find this entity in query: {expr}")
        else:
            raise NotImplementedError()

    cdef EntityType _find_entity(self, EntityType entity, bint allow_parent):
        if entity in self._entities:
            return entity

        # TODO: ambigous error
        # if is_entity_alias(entity) is False:
        #     for ent in self._entities:
        #         if entity is get_alias_target(ent):
        #             return ent

        if allow_parent is True and self._parent is not None:
            return self._parent._find_entity(entity, True)

        return None

    cdef str _get_next_alias(self):
        if self._parent is not None:
            return self._parent._get_next_alias()
        else:
            alias = f"t{self.__alias_c}"
            self.__alias_c += 1
            return alias

    cdef object _resolve_pending_joins(self):
        cdef int lenght = 0
        cdef int last_length = 0

        while True:
            pending_joins = self._pending_joins
            self._pending_joins = []
            for pending_joins in reversed(pending_joins):
                self.join(*(<tuple>pending_joins))

            length = len(self._pending_joins)
            if length == 0:
                break
            elif last_length == length:
                raise RuntimeError(f"Can't join the followings: {self._pending_joins}")
            last_length = lenght


    def __repr__(self):
        return QueryRepr().visit(self)


@cython.final
cdef class QueryLoad(Visitor):
    def __init__(self):
        self.entries = set()
        self.in_explicit = 0

    cdef object add(self, tuple input):
        self.in_explicit += 1
        for entry in input:
            if isinstance(entry, EntityType):
                self.entries.add(entry)
            else:
                self.visit(entry)
        self.in_explicit -= 1

    cdef QLS get(self, EntityAttribute attr):
        cdef QLS result = QLS.SKIP

        if attr._uid_ in self.entries:
            result = <QLS>(result | QLS.EXPLICIT)

        cdef EntityType entity = attr.get_entity()
        if entity in self.entries:
            result = <QLS>(result | QLS.IMPLICIT)

        cdef Loading loading = attr.get_ext(Loading)
        if loading is not None and loading.always is True:
            result = <QLS>(result | QLS.ALWAYS)

        return result


    def visit_field(self, Field field):
        self._add_entity_attr(field)

        if self.in_explicit > 0:
            if isinstance(field._impl_, CompositeImpl):
                self.entries.add((<CompositeImpl>field._impl_)._entity_)

    def visit_relation(self, Relation relation):
        cdef EntityType joined_ent = (<RelationImpl>relation._impl_).get_joined_alias()
        cdef Loading loading = relation.get_ext(Loading)

        self._add_entity_attr(relation)

        if loading is not None and loading.fields:
            for field in loading.fields:
                self.visit(getattr(joined_ent, field))

        if self.in_explicit > 0:
            self.entries.add(joined_ent)

    def visit_path(self, PathExpression path):
        cdef int last_index = len(path._path_) - 1
        cdef bint is_last

        for i in range(0, last_index):
            self.in_explicit -= 1
            self.visit(path._path_[i])
            self.in_explicit += 1

        self.visit(path._path_[last_index])

    def visit_related_attribute(self, RelatedAttribute related):
        self._add_entity_attr(related)
        self.visit(related.__rattr__)

    def visit_virtual_attr(self, VirtualAttribute vattr):
        if vattr._val is not None:
            self._add_entity_attr(vattr)

        elif vattr._deps is not None:
            # if has value expression, we dont need to load dependencies
            entity = vattr.get_entity()
            for field in vattr._deps:
                self.visit(getattr(entity, field))

    def visit_raw(self, expr):
        pass

    def visit_alias(self, expr):
        pass

    def visit_call(self, expr):
        pass

    def __default__(self, expr):
        if isinstance(expr, EntityAttribute):
            self._add_entity_attr(<EntityAttribute>expr)
        else:
            return super().__default__(expr)

    def __bool__(self):
        return bool(self.entries)

    def __len__(self):
        return len(self.entries)

    cdef _add_entity_attr(self, EntityAttribute attr):
        self.entries.add(attr._uid_)

    cdef QueryLoad clone(self):
        cdef QueryLoad result = QueryLoad()
        result.entries = set(self.entries)
        return result


# TODO: beautify
# cdef object load_options(dict target, tuple input):
#     for inp in input:
#         if isinstance(inp, Relation):
#             add_relation_to_load(target, <Relation>inp)
#             target[(<RelationImpl>(<Relation>inp)._impl_).get_joined_alias()] = inp
#         elif isinstance(inp, PathExpression):
#             pl = len((<PathExpression>inp)._path_)
#             for i, entry in enumerate((<PathExpression>inp)._path_):
#                 is_last = pl - 1 == i
#                 if isinstance(entry, Relation):
#                     add_relation_to_load(target, <Relation>entry)
#                     if is_last:
#                         target[(<RelationImpl>(<Relation>entry)._impl_).get_joined_alias()] = entry
#                 elif isinstance(entry, Field):
#                     if isinstance((<Field>entry)._impl_, CompositeImpl):
#                         if is_last:
#                             target[(<CompositeImpl>(<Field>entry)._impl_)._entity_] = entry

#                     target[(<Field>entry)._uid_] = entry
#                 elif isinstance(entry, RelatedAttribute):
#                     target[(<RelatedAttribute>entry)._uid_] = entry
#                 elif isinstance(entry, VirtualAttribute):
#                     add_virtual_attr_to_load(target, (<VirtualAttribute>entry))
#                 else:
#                     raise NotImplementedError(repr(entry))
#         elif isinstance(inp, VirtualAttribute):
#             add_virtual_attr_to_load(target, (<VirtualAttribute>inp))
#         elif isinstance(inp, EntityAttribute):
#             target[(<EntityAttribute>inp)._uid_] = inp
#         else:
#             target[inp] = inp


# cdef object add_relation_to_load(dict target, Relation relation):
#     cdef Loading loading = relation.get_ext(Loading)

#     target[relation._uid_] = relation

#     if loading is not None and loading.fields:
#         entity = (<RelationImpl>relation._impl_).get_joined_alias()
#         for field in loading.fields:
#             load_options(target, (getattr(entity, field),))


# cdef object add_virtual_attr_to_load(dict target, VirtualAttribute attr):
#     if attr._val is not None:
#         target[attr._uid_] = attr

#     elif attr._deps is not None:
#         # if has value expression, we dont need to load dependencies
#         entity = attr.get_entity()
#         for field in attr._deps:
#             load_options(target, (getattr(entity, field),))


@cython.final
@cython.freelist(1000)
cdef class RowConvertOp:
    def __cinit__(self, RCO op, object param1=None, object param2=None):
        self.op = op
        self.param1 = param1
        self.param2 = param2

    def __repr__(self):
        name = self.op

        if self.op == RCO.PUSH: name = "PUSH"
        elif self.op == RCO.POP: name = "POP"
        elif self.op == RCO.CREATE_STATE: name = "CREATE_STATE"
        elif self.op == RCO.CREATE_ENTITY: name = "CREATE_ENTITY"
        elif self.op == RCO.CREATE_POLYMORPH_ENTITY: name = "CREATE_POLYMORPH_ENTITY"
        elif self.op == RCO.CONVERT_SUB_ENTITY: name = "CONVERT_SUB_ENTITY"
        elif self.op == RCO.CONVERT_SUB_ENTITIES: name = "CONVERT_SUB_ENTITIES"
        elif self.op == RCO.SET_ATTR: name = "SET_ATTR"
        elif self.op == RCO.SET_ATTR_RECORD: name = "SET_ATTR_RECORD"
        elif self.op == RCO.GET_RECORD: name = "GET_RECORD"

        return "<RCO:%s %r %r>" % (name, self.param1, self.param2)


_RCO_PUSH = RowConvertOp(RCO.PUSH)
_RCO_POP = RowConvertOp(RCO.POP)


class VirtualFallback(Exception):
    def __init__(self, VirtualAttribute attr):
        self.attr = attr


cdef class QueryFinalizer(Visitor):
    def __cinit__(self, QueryCompiler compiler, Query q):
        self.q = q
        self.rcos = []
        self.in_or = 0
        self.compiler = compiler
        self.virtual_indexes = {}

    def visit_binary(self, BinaryExpression expr):
        if expr.op == operator.__or__:
            self.in_or += 1

        try:
            try:
                left = self.visit(expr.left)
            except VirtualFallback as vf:
                new_expr = VirtualBinaryExpression(vf.attr, expr.right, expr.op)
                new_expr.negated = expr.negated
                return self.visit(new_expr)

            try:
                right = self.visit(expr.right)
            except VirtualFallback as vf:
                new_expr = VirtualBinaryExpression(left, vf.attr, expr.op)
                new_expr.negated = expr.negated
                return self.visit(new_expr)

            if expr.negated:
                return ~expr.op(left, right)
            else:
                return expr.op(left, right)
        finally:
            if expr.op == operator.__or__:
                self.in_or -= 1


    def visit_unary(self, UnaryExpression expr):
        return expr.op(self.visit(expr.expr))

    def visit_cast(self, CastExpression expr):
        return CastExpression(self.visit(expr.expr), expr.to)

    def visit_order(self, OrderExpression expr):
        try:
            res = self.visit(expr.expr)
        except VirtualFallback as vf:
            new_expr = VirtualOrderExpression(vf.attr, expr.is_asc)
            return self.visit(new_expr)

        if expr.is_asc:
            return res.asc()
        else:
            return res.desc()

    def visit_call(self, CallExpression expr):
        return CallExpression(self.visit(expr.callable), self._visit_iterable(expr.args))

    def visit_raw(self, expr):
        return expr

    def visit_field(self, Field expr):
        cdef EntityType expr_entity = expr.get_entity()
        if self.q._find_entity(expr_entity, True) is None:
            self.q.join(expr_entity, type="LEFT" if self.in_or > 0 else "INNER")
        return expr

    def visit_const(self, expr):
        return expr

    def visit_query(self, expr):
        cdef Query result = expr.clone()
        result._allow_clone = False
        result._parent = self.q
        return result.finalize(self.compiler)[0]

    def visit_alias(self, AliasExpression expr):
        return self.visit(expr.expr).alias(expr.value)

    def visit_path(self, PathExpression expr):
        for i, entry in enumerate(expr._path_):
            if isinstance(entry, VirtualAttribute):
                if len(expr._path_) - 1 != i:
                    raise RuntimeError("Not implemented, more path entries after virtual expression")

                new_attr = (<VirtualAttribute>entry).with_path(PathExpression(expr._path_[0:i]))
                raise VirtualFallback(new_attr)

        if isinstance(expr._path_[0], Relation):
            for p in expr._path_:
                if isinstance(p, Relation):
                    self.q.join(p, type="LEFT" if self.in_or > 0 else "INNER")
                elif isinstance(p, RelatedAttribute):
                    # TODO: not join poly child if next path entry is in base entity
                    self.q.join((<RelatedAttribute>p).__relation__, type="LEFT" if self.in_or > 0 else "INNER")
                else:
                    break
        return PathExpression(list(expr._path_))

    def visit_related_attribute(self, RelatedAttribute expr):
        return self.visit(expr.__rpath__)

    def visit_virtual_attr(self, VirtualAttribute expr):
        return self.visit(expr.get_value_expr(self.q))

    def visit_virtual_binary(self, VirtualBinaryExpression expr):
        return self.visit(expr._create_expr_(self.q))

    def visit_virtual_order(self, VirtualOrderExpression expr):
        cdef VirtualAttribute attr = expr.expr

        if attr._order:
            return self.visit(expr._create_expr_(self.q))
        else:
            try:
                idx = self._find_column_index(attr)
            except ValueError:
                return self.visit(expr._create_expr_(self.q))
            else:
                col_ref = ColumnRefExpression(expr.expr, idx)
                if expr.is_asc:
                    return col_ref.asc()
                else:
                    return col_ref.desc()

    def visit_relation(self, expr):
        return expr

    def finalize(self, *expr_list):
        if self.q._select_from:
            new_from = []
            for f in self.q._select_from:
                if isinstance(f, EntityType):
                    new_from.append(f)
                else:
                    new_from.append(self.visit(f))
            self.q._select_from = new_from

        if self.q._columns:
            if not self.q._load:
                self.q.load(*self.q._columns)
            self._visit_columns(list(self.q._columns))
        else:
            if not self.q._load:
                self.q.load(*self.q._select_from)
            self._visit_columns(self.q._select_from)

        if self.q._where:
            self.q._where = self._visit_iterable(self.q._where)

        if self.q._order:
            self.q._order = self._visit_iterable(self.q._order)

        if self.q._group:
            self.q._group = self._visit_iterable(self.q._group)

        if self.q._having:
            self.q._having = self._visit_iterable(self.q._having)

        if self.q._distinct:
            self.q._distinct = self._visit_iterable(self.q._distinct)

        self.q._resolve_pending_joins()

        self.q._rcos = self.rcos

        # print("="*40)
        # from pprint import pprint
        # pprint(self.rcos)
        # print("="*40)

    def _visit_columns(self, expr_list):
        cdef PathExpression path
        cdef EntityType entity
        self.q._columns = []

        for expr in expr_list:
            if isinstance(expr, EntityType):
                self.rcos.append(self._rco_for_entity(<EntityType>expr))
            elif isinstance(expr, PathExpression):
                path = <PathExpression>expr
                last_entry = path._path_[len(path._path_) - 1]

                if isinstance(last_entry, Field):
                    primary_field = path._path_[0]
                    pstart = 1
                    if isinstance(primary_field, Relation):
                        primary_field = path._path_[1]
                        pstart = 2

                    _path = []
                    for i in range(pstart, len(path._path_)):
                        p = path._path_[i]
                        _path.append(p._key_)

                    if isinstance((<Field>last_entry)._impl_, CompositeImpl):
                        self.rcos.append(self._rco_for_composite(primary_field, (<CompositeImpl>(<Field>last_entry)._impl_)._entity_, _path))
                        self.visit(expr)
                    else:
                        self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
                        self.q._columns.append(self.visit(expr))
                elif isinstance(last_entry, VirtualAttribute):
                    new_vattr = (<VirtualAttribute>last_entry).with_path(PathExpression(path._path_[0:len(path._path_) - 1]))
                    self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
                    self.virtual_indexes[(<VirtualAttribute>new_vattr)._uid_] = len(self.q._columns)
                    self.q._columns.append(self.visit(new_vattr))
            elif isinstance(expr, Field):
                if isinstance((<Field>expr)._impl_, CompositeImpl):
                    self.rcos.append(self._rco_for_composite((<Field>expr), (<CompositeImpl>(<Field>expr)._impl_)._entity_, []))
                    self.visit(expr)
                else:
                    self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
                    self.q._columns.append(self.visit(expr))
            elif isinstance(expr, VirtualAttribute):
                if (<VirtualAttribute>expr)._val:
                    self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
                    self.virtual_indexes[(<VirtualAttribute>expr)._uid_] = len(self.q._columns)
                    self.q._columns.append(self.visit(expr))
            else:
                self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
                self.q._columns.append(self.visit(expr))


    def _rco_for_entity(self, EntityType entity_type, dict existing=None, list before_create=[]):
        cdef Polymorph polymorph = entity_type.__polymorph__

        if existing is None:
            existing = {}

        if polymorph is not None:
            return self._rco_for_poly_entity(entity_type, polymorph, existing, before_create)
        else:
            return self._rco_for_normal_entity(entity_type, existing, before_create)

    def _rco_for_normal_entity(self, EntityType entity_type, dict existing=None, list before_create=[]):
        cdef EntityType aliased = get_alias_target(entity_type)
        cdef EntityType attr_entity
        cdef list rco = [RowConvertOp(RCO.CREATE_STATE, aliased)]
        cdef EntityAttribute attr
        cdef Field field
        cdef relation_rco = []
        cdef Loading loading

        cdef QueryLoad load_attrs = self.q._load
        cdef QLS load_source

        for attr in entity_type.__attrs__:
            load_source = load_attrs.get(attr)
            if load_source == QLS.SKIP:
                continue

            if isinstance(attr, Field):
                field = <Field>attr

                if isinstance(field._impl_, CompositeImpl):
                    rco[0:0] = self._rco_for_composite(field, (<CompositeImpl>field._impl_)._entity_)
                    rco.append(_RCO_POP)
                    rco.append(RowConvertOp(RCO.SET_ATTR, aliased.__fields__[field._index_]))
                else:
                    try:
                        idx = existing[field._uid_]
                    except KeyError:
                        try:
                            idx = self._find_column_index(field)
                        except ValueError:
                            idx = len(self.q._columns)
                            self.q._columns.append(field)
                            existing[field._uid_] = idx

                    rco.append(RowConvertOp(RCO.SET_ATTR_RECORD, aliased.__fields__[field._index_], idx))
            elif isinstance(attr, Relation):
                # must have explicit load for relations
                if load_source & (QLS.EXPLICIT | QLS.ALWAYS):
                    if isinstance((<Relation>attr)._impl_, ManyToOne):
                        relation_rco.append((<Relation>attr, self._rco_for_one_relation(<Relation>attr, existing)))
                    else:
                        relation_rco.append((<Relation>attr, self._rco_for_many_relation(<Relation>attr)))
            elif isinstance(attr, VirtualAttribute):
                if not (<VirtualAttribute>attr)._val:
                    continue

                if load_source & (QLS.EXPLICIT | QLS.ALWAYS):
                    try:
                        idx = existing[attr._uid_]
                    except KeyError:
                        try:
                            idx = self._find_column_index(attr)
                        except ValueError:
                            idx = len(self.q._columns)
                            self.q._columns.append(self.visit(attr))
                            existing[attr._uid_] = idx

                    self.virtual_indexes[attr._uid_] = idx

                    # not optimal, but working
                    rco.append(RowConvertOp(RCO.GET_RECORD, idx))
                    rco.append(_RCO_PUSH)
                    rco.append(_RCO_POP)
                    rco.append(RowConvertOp(RCO.SET_ATTR, aliased.__attrs__[attr._index_]))

        for rel, relco in relation_rco:
            if relco is not None:
                rco[0:0] = relco
                rco.append(_RCO_POP)
                rco.append(RowConvertOp(RCO.SET_ATTR, rel))

        # TODO: ne hozzon létre üres entityt, nincs értelme
        # if len(rco) == 1:
        #     return []

        rco.extend(before_create)
        rco.append(RowConvertOp(RCO.CREATE_ENTITY, aliased))
        return rco

    def _rco_for_poly_entity(self, EntityType entity, Polymorph poly, dict fields, list before_create=None):
        cdef list parents = poly.parents()
        # cdef EntityType ent
        cdef EntityType root_entity
        cdef EntityType parent_entity
        cdef Relation relation
        cdef Relation parent_relation = None
        cdef Field field
        cdef list rco = []
        cdef dict create_poly = {}
        cdef tuple pk_fields
        cdef list poly_id_fields = []

        if len(parents) != 0:
            for relation in parents:
                self.q.join(relation, None, "INNER")

            relation = parents[len(parents) - 1]
            root_entity = (<RelationImpl>relation._impl_).get_joined_alias()
        else:
            root_entity = entity

        pk_fields = root_entity.__pk__
        for ent_id in poly.info.id_fields:
            poly_id_fields.append(getattr(root_entity, ent_id))
            self.q.load(*poly_id_fields)

        for relation in reversed(parents):
            if parent_relation is not None:
                before_create = [
                    _RCO_POP,
                    RowConvertOp(RCO.SET_ATTR, parent_relation),
                ]
            else:
                before_create = []
            parent_relation = relation

            parent_entity = (<RelationImpl>relation._impl_).get_joined_alias()

            for i, field in enumerate(pk_fields):
                try:
                    pkidx = fields[field._uid_]
                except KeyError:
                    pass
                else:
                    fields[parent_entity.__pk__[i]._uid_] = pkidx

            self.q.load(parent_entity)
            rco.extend(self._rco_for_normal_entity(parent_entity, fields, before_create))
            rco.append(_RCO_PUSH)

        if parent_relation is not None:
            before_create = [
                _RCO_POP,
                RowConvertOp(RCO.SET_ATTR, parent_relation),
            ]

            for i, field in enumerate(pk_fields):
                fields[entity.__pk__[i]._uid_] = fields[field._uid_]
        else:
            before_create = []

            for i, field in enumerate(pk_fields):
                try:
                    pkidx = fields[field._uid_]
                except KeyError:
                    pass
                else:
                    fields[entity.__pk__[i]._uid_] = pkidx

        rco.extend(self._rco_for_normal_entity(entity, fields, before_create))

        pc = self._add_poly_child(create_poly, entity, fields, pk_fields)
        if pc is True:
            rco.append(_RCO_PUSH)

            id_fields = []
            for field in poly_id_fields:
                id_fields.append(fields[field._uid_])

            rco.append(RowConvertOp(RCO.CREATE_POLYMORPH_ENTITY, tuple(id_fields), create_poly))

        return rco

    def _add_poly_child(self, dict create_poly, EntityType entity, dict fields, tuple pk_fields):
        cdef Polymorph poly = entity.__polymorph__
        cdef Relation relation
        cdef EntityType child
        cdef bint has_poly_child = False
        cdef dict child_rcos = {}
        cdef Field field

        for relation in poly.children:
            child = (<RelationImpl>relation._impl_).get_joined_alias()
            self.q.join(relation, None, type="LEFT")

            for i, field in enumerate(pk_fields):
                try:
                    pkidx = fields[field._uid_]
                except KeyError:
                    pass
                else:
                    fields[child.__pk__[i]._uid_] = pkidx

            self.q.load(child)
            rcos = self._rco_for_normal_entity(child, fields, [
                _RCO_POP,
                RowConvertOp(RCO.SET_ATTR, child.__polymorph__.parent),
            ])

            pc = self._add_poly_child(child_rcos, child, fields, pk_fields)
            if pc:
                for k, v in child_rcos.items():
                    v[0][0:0] = rcos + [_RCO_PUSH]

                create_poly.update(child_rcos)

            create_poly[poly.get_id(child)] = [rcos]
            has_poly_child = True

        return has_poly_child

    def _rco_for_composite(self, Field field, EntityType entity, list path=None):
        cdef Field f
        cdef list rco = [RowConvertOp(RCO.CREATE_STATE, entity)]

        src = field
        if path:
            for p in path:
                src = getattr(src, p)
        else:
            path = []

        for f in entity.__fields__:
            if isinstance(f._impl_, CompositeImpl):
                rco[0:0] = self._rco_for_composite(field, (<CompositeImpl>f._impl_)._entity_, path + [f._key_])
                rco.append(_RCO_POP)
                rco.append(RowConvertOp(RCO.SET_ATTR, f))
            else:
                idx = len(self.q._columns)
                self.q._columns.append(getattr(src, f._name_))
                rco.append(RowConvertOp(RCO.SET_ATTR_RECORD, f, idx))

        rco.append(RowConvertOp(RCO.CREATE_ENTITY, entity, True))
        rco.append(_RCO_PUSH)
        return rco

    # TODO: optimize with joins instead of subquerry
    def _rco_for_one_relation(self, Relation relation, dict existing=None):
        cdef EntityType load = (<RelationImpl>relation._impl_).get_joined_alias()
        cdef Query col_query = Query(load).where((<RelationImpl>relation._impl_).join_expr).as_row()

        if self.q._load:
            col_query._load = self.q._load.clone()

        column_name = self.q._get_next_alias()
        cdef AliasExpression column_alias = self.visit(col_query.alias(column_name))
        cdef Query column = column_alias.expr

        col_idx = len(self.q._columns)
        self.q._columns.append(column_alias)

        return [RowConvertOp(RCO.CONVERT_SUB_ENTITY, col_idx, column._rcos), _RCO_PUSH]

    def _rco_for_many_relation(self, Relation relation, dict existing=None):
        cdef ManyToMany rimm
        cdef EntityType load = (<RelationImpl>relation._impl_).get_joined_alias()
        cdef Query q

        if isinstance(relation._impl_, ManyToMany):
            rimm = <ManyToMany>relation._impl_
            q = Query(rimm.get_across_alias()) \
                .columns(rimm.get_joined_alias()) \
                .join(rimm.get_joined_alias(), rimm.join_expr, "INNER") \
                .where(rimm.across_join_expr)
        else:
            q = Query(load).where((<RelationImpl>relation._impl_).join_expr)

        if self.q._load:
            q._load = self.q._load.clone()

        column_name = self.q._get_next_alias()
        alias_name = self.q._get_next_alias()
        col_query = Query(q.alias(alias_name)).columns(raw(f'ARRAY_AGG("{alias_name}")'))
        cdef AliasExpression column_alias = self.visit(col_query.alias(column_name))
        cdef Query column = column_alias.expr
        cdef AliasExpression subq_alias = column._select_from[0]
        cdef Query subq = subq_alias.expr

        col_idx = len(self.q._columns)
        self.q._columns.append(column_alias)

        return [RowConvertOp(RCO.CONVERT_SUB_ENTITIES, col_idx, subq._rcos), _RCO_PUSH]

    def _find_column_index(self, EntityAttribute field):
        for i, c in enumerate(self.q._columns):
            if isinstance(c, EntityAttribute) and (<EntityAttribute>c)._uid_ is field._uid_:
                return i

        try:
            return self.virtual_indexes[field._uid_]
        except KeyError:
            pass

        raise ValueError()


cdef inline determine_join(Query q, EntityType joined):
    cdef EntityType ent

    for ent in q._entities:
        try:
            return determine_join_expr(ent, joined)
        except JoinError:
            try:
                return determine_join_expr(joined, ent)
            except JoinError:
                continue

    raise JoinError("Can't found suitable join condition between %r <-> %r" % (q, joined))


@cython.final
cdef class QueryFactory:
    """
    TODO: refactor plan
    1. replace fileds with special ConstExpression
    2. finalize query
    3. compile query
    4. create mapping between special ConstExpression and record index
    5. rco...
    6. REMOVE QueryFactory
    7. REMOVE RCO.LOAD_ONE_ENTITY
    8. REMOVE RCO.CONVERT_SUB_ENTITIES
    """

    def __init__(self, Query query, tuple fields, Expression join_expr):
        self.query = query
        self.fields = fields
        self.join_expr = join_expr

    def __call__(self, tuple values):
        return self.query.clone().where(replace_fields(self.join_expr, self.fields, values))

    def __repr__(self):
        return "<QueryFactory %r %r>" % (self.query._select_from, self.join_expr)


class FieldConstExpr(ConstExpression):
    pass


cdef class QueryCompiler:
    def __cinit__(self, Dialect dialect):
        self.dialect = dialect

    cpdef compile_select(self, Query query):
        raise NotImplementedError()

    cpdef compile_insert(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()

    cpdef compile_insert_or_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()

    cpdef compile_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()

    cpdef compile_delete(self, EntityType entity, list attrs, list names, list values, bint inline_values=False):
        raise NotImplementedError()



#  if self._columns:
#             parts.append(f"{indent}SELECT:")
#             for col in self._columns:
#                 parts.append(f"{indent}{indent_char}{col}")

#         if self._select_from:
#             parts.append(f"{indent}FROM:")
#             parts.append(f"{indent}{indent_char}{self._select_from}")

#         if self._joins:
#             parts.append(f"{indent}JOINS:")
#             for what, cond, type in self._joins.values():
#                 parts.append(f"{indent}{indent_char}{type} {what} ON {cond}")

#         if self._where:
#             parts.append(f"{indent}WHERE:")
#             for value in self._where:
#                 parts.append(f"{indent}{indent_char}{value}")

#         if self._order:
#             parts.append(f"{indent}ORDER:")
#             for value in self._order:
#                 parts.append(f"{indent}{indent_char}{value}")

#         if self._group:
#             parts.append(f"{indent}GROUP:")
#             for value in self._group:
#                 parts.append(f"{indent}{indent_char}{value}")

#         if self._having:
#             parts.append(f"{indent}HAVING:")
#             for value in self._having:
#                 parts.append(f"{indent}{indent_char}{value}")

#         if self._range:
#             parts.append(f"{indent}RANGE: {self._range}")


class QueryRepr(Visitor):
    def __init__(self, int level=0):
        self.level = level
        self.indent_char = "    "

    def visit_query(self, Query query):
        self.level += 1
        if self.level > 1:
            begin = self.__indent("(\n")
            end = self.__indent("\n)")
        else:
            begin = ""
            end = ""

        cdef list parts = []

        if query._columns:
            parts.append(self.__indent("SELECT"))
            parts.append(self.__visit_iterable(query._columns, 1))

        if query._select_from:
            parts.append(self.__indent(f"FROM: {query._select_from}"))

        if query._joins:
            parts.append(self.__indent("JOINS"))
            parts.append(self.__visit_joins(query._joins.values(), 1))

        if query._where:
            parts.append(self.__indent("WHERE"))
            parts.append(self.__visit_iterable(query._where, 1))

        if query._order:
            parts.append(self.__indent("ORDER"))
            parts.append(self.__visit_iterable(query._order, 1))

        if query._group:
            parts.append(self.__indent("GROUP"))
            parts.append(self.__visit_iterable(query._order, 1))

        if query._having:
            parts.append(self.__indent("HAVING"))
            parts.append(self.__visit_iterable(query._having, 1))

        if query._range:
            parts.append(self.__indent(f"RANGE {query._range}"))

        self.level -= 1

        return begin + "\n".join(parts) + end

    def visit_alias(self, expr):
        return f"{self.visit((<AliasExpression>expr).expr)} AS {(<AliasExpression>expr).value}"

    def __visit_joins(self, joins, extra_level=0):
        self.level += extra_level
        cdef list result = []
        for what, cond, type in joins:
            result.append(self.__indent(f"{type} {what} ON {self.visit(cond)}"))
        self.level -= extra_level
        return "\n".join(result)

    def __visit_iterable(self, expr, extra_level=0):
        self.level += extra_level
        result = self._visit_iterable(expr)
        indent = self.indent_char * self.level
        self.level -= extra_level
        sep = f"{indent}\n"
        return f"{indent}{sep.join(result)}"

    def __default__(self, expr):
        return repr(expr)

    def __indent(self, value):
        return f"{self.indent_char * self.level}{value}"

