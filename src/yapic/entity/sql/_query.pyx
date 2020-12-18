import operator
import cython

from yapic.entity._entity cimport EntityType, EntityAttribute, PolymorphMeta, get_alias_target, is_entity_alias
from yapic.entity._field cimport Field, field_eq
from yapic.entity._field_impl cimport CompositeImpl
from yapic.entity._expression cimport (Expression, AliasExpression, DirectionExpression, Visitor, BinaryExpression,
    UnaryExpression, CastExpression, CallExpression, RawExpression, PathExpression,
    VirtualExpressionVal, VirtualExpressionBinary, VirtualExpressionDir, ConstExpression, raw)
from yapic.entity._expression import and_
from yapic.entity._relation cimport Relation, RelationImpl, ManyToOne, ManyToMany, RelatedAttribute, determine_join_expr, Loading
from yapic.entity._error cimport JoinError
from yapic.entity._visitors cimport extract_fields, replace_fields, replace_entity, ReplacerBase
from yapic.entity._virtual_attr cimport VirtualAttribute

from ._dialect cimport Dialect


cdef class Query(Expression):
    def __cinit__(self):
        self._entities = []
        self._load = {}
        self._exclude = {}
        self._parent = None
        self._allow_clone = True
        self.__expr_alias = {}
        self.__alias_c = 0

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

        if isinstance(from_, EntityType) and from_ not in self._entities:
            self._entities.append(from_)

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
                    or isinstance(col, VirtualExpressionVal) \
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
            if isinstance(item, (DirectionExpression, RawExpression, VirtualExpressionDir)):
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
            (<Relation>what).update_join_expr()
            impl = (<Relation>what)._impl_

            if isinstance(impl, ManyToMany):
                cross_condition = (<ManyToMany>impl).across_join_expr
                cross_what = (<ManyToMany>impl).across

                if cross_what not in self._entities:
                    self._entities.append(cross_what)

                try:
                    existing = self._joins[cross_what]
                except KeyError:
                    self._joins[cross_what] = [cross_what, cross_condition, type]
                else:
                    if type.upper().startswith("INNER"):
                        existing[2] = type
                type = "INNER"

            condition = impl.join_expr
            entity = impl.joined

            if entity is None:
                raise RuntimeError("Relation is deferred: %r" % what)

            if condition is None:
                raise RuntimeError("Missing join expression from relation: %r" % what)

            if entity in self._select_from or entity in self._entities:
                return self

        elif isinstance(what, EntityType):
            if what in self._select_from or what in self._entities:
                return self

            entity = <EntityType>what

            if condition is None:
                condition = determine_join(self, entity)

        self._entities.append(entity)

        # TODO: Nem biztos, hogy ezt így kéne...
        aliased = get_alias_target(entity)
        if aliased not in self._entities:
            self._entities.append(aliased)

        try:
            existing = self._joins[entity]
        except KeyError:
            self._joins[entity] = [entity, condition, type]
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
        self._load = {}
        return self

    def reset_exclude(self):
        self._exclude = {}
        return self

    def load(self, *load):
        # for entry in load:
        #     if isinstance(entry, VirtualExpressionVal) and (<VirtualExpressionVal>entry)._virtual_._val:
        #         self._load[(<VirtualExpressionVal>entry)._virtual_._uid_] = (<VirtualExpressionVal>entry)._create_expr_(self)

        load_options(self._load, load)
        return self

    def exclude(self, *exclude):
        load_options(self._exclude, exclude)
        return self

    cpdef Query clone(self):
        if not self._allow_clone:
            raise RuntimeError("Query is not cloneable")

        cdef Query q = type(self)()

        if self._select_from: q._select_from = list(self._select_from)
        if self._columns:     q._columns = list(self._columns)
        if self._where:       q._where = list(self._where)
        if self._order:       q._order = list(self._order)
        if self._group:       q._group = list(self._group)
        if self._having:      q._having = list(self._having)
        if self._distinct:    q._distinct = list(self._distinct)
        if self._prefix:      q._prefix = list(self._prefix)
        if self._suffix:      q._suffix = list(self._suffix)
        if self._joins:       q._joins = dict(self._joins)
        if self._range:       q._range = slice(self._range.start, self._range.stop, self._range.step)
        if self._entities:    q._entities = list(self._entities)
        if self._load:        q._load = dict(self._load)
        if self._exclude:     q._exclude = dict(self._exclude)
        if self._parent:      q._parent = self._parent.clone()
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
                if self._entity_reachable(expr, False):
                    original = get_alias_target(expr)
                    if original is not expr and expr.__name__:
                        alias = expr.__name__
                    else:
                        alias = self._get_next_alias()

                    self.__expr_alias[expr] = alias
                    # TODO: Nem biztos, hogy ezt így kéne...
                    self.__expr_alias[original] = alias
                    return alias
                elif self._parent:
                    return self._parent.get_expr_alias(expr)
                else:
                    raise ValueError(f"Can't find this entity in query: {expr}")
        else:
            raise NotImplementedError()

    cdef bint _entity_reachable(self, EntityType entity, bint allow_parent):
        return entity in self._entities \
            or (allow_parent and self._parent is not None and self._parent._entity_reachable(entity, allow_parent))

    cdef str _get_next_alias(self):
        if self._parent is not None:
            return self._parent._get_next_alias()
        else:
            alias = f"t{self.__alias_c}"
            self.__alias_c += 1
            return alias


# TODO: beautify
cdef object load_options(dict target, tuple input):
    for inp in input:
        if isinstance(inp, Relation):
            target[(<Relation>inp)._uid_] = inp
            target[(<Relation>inp)._impl_.joined] = inp
        elif isinstance(inp, EntityAttribute):
            target[(<EntityAttribute>inp)._uid_] = inp
        elif isinstance(inp, VirtualExpressionVal):
            if (<VirtualExpressionVal>inp)._virtual_._val:
                target[(<VirtualExpressionVal>inp)._virtual_._uid_] = inp
        elif isinstance(inp, PathExpression):
            pl = len((<PathExpression>inp)._path_)
            for i, entry in enumerate((<PathExpression>inp)._path_):
                is_last = pl - 1 == i
                if isinstance(entry, Relation):
                    target[(<Relation>entry)._uid_] = entry
                    if is_last:
                        target[(<Relation>entry)._impl_.joined] = entry
                elif isinstance(entry, Field):
                    if isinstance((<Field>entry)._impl_, CompositeImpl):
                        if is_last:
                            target[(<Field>entry)._impl_._entity_] = entry

                    target[(<Field>entry)._uid_] = entry
                elif isinstance(entry, RelatedAttribute):
                    target[(<RelatedAttribute>entry)._uid_] = entry
                else:
                    raise NotImplementedError(repr(entry))
        else:
            target[inp] = inp


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


cdef class QueryFinalizer(Visitor):
    def __cinit__(self, QueryCompiler compiler, Query q):
        self.q = q
        self.rcos = []
        self.in_or = 0
        self.compiler = compiler

    def visit_binary(self, BinaryExpression expr):
        if expr.op == operator.__or__:
            self.in_or += 1

        try:
            if expr.negated:
                return ~expr.op(self.visit(expr.left), self.visit(expr.right))
            else:
                return expr.op(self.visit(expr.left), self.visit(expr.right))
        finally:
            if expr.op == operator.__or__:
                self.in_or -= 1

    def visit_unary(self, UnaryExpression expr):
        return expr.op(self.visit(expr.expr))

    def visit_cast(self, CastExpression expr):
        return CastExpression(self.visit(expr.expr), expr.to)

    def visit_direction(self, DirectionExpression expr):
        cdef Expression res = self.visit(expr.expr)
        if expr.is_asc:
            return res.asc()
        else:
            return res.desc()

    def visit_call(self, CallExpression expr):
        return CallExpression(self.visit(expr.callable), self._visit_list(expr.args))

    def visit_raw(self, expr):
        return expr

    def visit_field(self, expr):
        if not self.q._entity_reachable(expr._entity_, True):
            self.q.join(expr._entity_, type="LEFT" if self.in_or > 0 else "INNER")
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
        if isinstance(expr._path_[0], Relation):
            for p in expr._path_:
                if isinstance(p, Relation):
                    self.q.join(p, type="LEFT" if self.in_or > 0 else "INNER")
                else:
                    break

        return PathExpression(list(expr._path_))

    def visit_vexpr_val(self, VirtualExpressionVal expr):
        return self.visit(expr._create_expr_(self.q))

    def visit_vexpr_binary(self, VirtualExpressionBinary expr):
        return self.visit(expr._create_expr_(self.q))

    def visit_vexpr_dir(self, VirtualExpressionDir expr):
        return self.visit(expr._create_expr_(self.q))

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

        # if self.q._load:
        #     load = {}
        #     for k, v in self.q._load.items():
        #         if isinstance(v, (VirtualExpressionVal, VirtualExpressionBinary)):
        #             load[k] = self.visit(v)
        #         else:
        #             load[k] = v
        #     self.q._load = load

        if self.q._columns:
            if not self.q._load:
                self.q.load(*self.q._columns)
            self._visit_columns(list(self.q._columns))
        else:
            if not self.q._load:
                self.q.load(*self.q._select_from)
            self._visit_columns(self.q._select_from)

        if self.q._where:
            self.q._where = self._visit_list(self.q._where)

        if self.q._order:
            self.q._order = self._visit_list(self.q._order)

        if self.q._group:
            self.q._group = self._visit_list(self.q._group)

        if self.q._having:
            self.q._having = self._visit_list(self.q._having)

        if self.q._distinct:
            self.q._distinct = self._visit_list(self.q._distinct)

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
            elif isinstance(expr, Field):
                if isinstance((<Field>expr)._impl_, CompositeImpl):
                    self.rcos.append(self._rco_for_composite((<Field>expr), (<CompositeImpl>(<Field>expr)._impl_)._entity_, []))
                    self.visit(expr)
                else:
                    self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
                    self.q._columns.append(self.visit(expr))
            else:
                self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
                self.q._columns.append(self.visit(expr))

    def _visit_list(self, expr_list):
        cdef list res = []
        for expr in expr_list:
            res.append(self.visit(expr))
        return res

    # def _select_entity(self, EntityType entity, dict fields={}):
    #     cdef PolymorphMeta polymorph = entity.__meta__.get("polymorph", None)

    #     if fields is None:
    #         fields = {}

    #     if polymorph:
    #         self.rcos.append(self._select_polymorph(entity, polymorph, fields))
    #     else:
    #         self.rcos.append(self._rco_for_entity(entity))

    # def _select_polymorph(self, EntityType entity, PolymorphMeta poly, dict fields):




    def _rco_for_entity(self, EntityType entity_type, dict existing=None, list before_create=[]):
        cdef PolymorphMeta polymorph = entity_type.__meta__.get("polymorph", None)

        if existing is None:
            existing = {}

        if polymorph:
            return self._rco_for_poly_entity(entity_type, polymorph, existing, before_create)
        else:
            return self._rco_for_normal_entity(entity_type, existing, before_create)

    def _rco_for_normal_entity(self, EntityType entity_type, dict existing=None, list before_create=[]):
        cdef EntityType aliased = get_alias_target(entity_type)
        cdef list rco = [RowConvertOp(RCO.CREATE_STATE, aliased)]
        cdef EntityAttribute attr
        cdef Field field
        cdef Relation relation
        cdef relation_rco = []
        cdef Loading loading

        for attr in entity_type.__attrs__:
            if isinstance(attr, Field):
                field = <Field>attr

                if ((field._uid_ in self.q._load or field._entity_ in self.q._load)
                        and (not self.q._exclude
                            or field._uid_ not in self.q._exclude
                            or field._entity_ not in self.q._exclude)):

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
                loading = <Loading>attr.get_ext(Loading)
                if attr._uid_ in self.q._load or (loading is not None and loading.always):
                    relation = <Relation>attr
                    relation.update_join_expr()

                    if loading is not None and loading.always:
                        if loading.fields:
                            joined_entity = relation._impl_.joined
                            for fname in loading.fields:
                                self.q.load(getattr(joined_entity, fname))
                        else:
                            self.q.load(relation._impl_.joined)

                    if isinstance(relation._impl_, ManyToOne):
                        relation_rco.append((relation, self._rco_for_one_relation(relation, existing)))
                    else:
                        relation_rco.append((relation, self._rco_for_many_relation(relation)))
            elif isinstance(attr, VirtualAttribute) and attr._uid_ in self.q._load:
                try:
                    idx = existing[attr._uid_]
                except KeyError:
                    try:
                        idx = self._find_column_index(attr)
                    except ValueError:
                        idx = len(self.q._columns)
                        self.q._columns.append(self.visit(self.q._load[attr._uid_]))
                        existing[attr._uid_] = idx

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

    def _rco_for_poly_entity(self, EntityType entity, PolymorphMeta poly, dict fields, list before_create=None):
        cdef list parents = poly.parents(entity)
        # cdef EntityType ent
        cdef EntityType entity_tmp
        cdef Relation relation
        cdef Relation parent_relation = None
        cdef Field field
        cdef list rco = []
        cdef dict create_poly = {}
        cdef tuple pk_fields
        cdef list poly_id_fields = []

        # cdef str parent_join_type = "INNER"
        # if self.q._joins and entity in self.q._joins:
        #     parent_join_type = self.q._joins[entity][2]

        if len(parents) != 0:
            # TODO: Szerintem ez nem kell ide
            for relation in parents:
                self.q.join(relation, None, "INNER")

            relation = parents[len(parents) - 1]
            entity_tmp = relation._impl_.joined
        else:
            entity_tmp = entity

        pk_fields = entity_tmp.__pk__
        for ent_id in poly.id_fields:
            poly_id_fields.append(getattr(entity_tmp, ent_id))
            self.q.load(*poly_id_fields)

        for relation in reversed(parents):
            self.q.join(relation, None, "INNER")

            if parent_relation:
                before_create = [
                    _RCO_POP,
                    RowConvertOp(RCO.SET_ATTR, parent_relation),
                ]
            else:
                before_create = []
            parent_relation = relation

            entity_tmp = relation._impl_.joined

            for i, field in enumerate(pk_fields):
                try:
                    pkidx = fields[field._uid_]
                except KeyError:
                    pass
                else:
                    fields[entity_tmp.__pk__[i]._uid_] = pkidx

            self.q.load(entity_tmp)
            rco.extend(self._rco_for_normal_entity(entity_tmp, fields, before_create))
            rco.append(_RCO_PUSH)

        if parent_relation:
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

        entity_tmp = get_alias_target(entity)
        pc = self._add_poly_child(create_poly, entity_tmp, poly, fields, pk_fields)
        if pc is True:
            rco.append(_RCO_PUSH)

            id_fields = []
            for field in poly_id_fields:
                id_fields.append(fields[field._uid_])

            rco.append(RowConvertOp(RCO.CREATE_POLYMORPH_ENTITY, tuple(id_fields), create_poly))

        return rco

    def _add_poly_child(self, dict create_poly, EntityType entity, PolymorphMeta poly, dict fields, tuple pk_fields):
        cdef Relation relation
        cdef EntityType child
        cdef bint has_poly_child = False
        cdef dict child_rcos = {}
        cdef Field field

        for relation in poly.children(entity):
            relation.update_join_expr()
            child = relation._entity_
            self.q.join(child, relation._default_, "LEFT")

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
                RowConvertOp(RCO.SET_ATTR, relation),
            ])

            pc = self._add_poly_child(child_rcos, child, poly, fields, pk_fields)
            if pc:
                for k, v in child_rcos.items():
                    v[0][0:0] = rcos + [_RCO_PUSH]

                create_poly.update(child_rcos)

            create_poly[poly.entities[child][0]] = [rcos]
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

    def _rco_for_one_relation(self, Relation relation, dict existing=None):
        cdef EntityType load = relation._impl_.joined
        cdef Query col_query = Query(load).where(relation._impl_.join_expr).as_row()

        if self.q._load:
            col_query._load = dict(self.q._load)

        column_name = self.q._get_next_alias()
        cdef AliasExpression column_alias = self.visit(col_query.alias(column_name))
        cdef Query column = column_alias.expr

        col_idx = len(self.q._columns)
        self.q._columns.append(column_alias)

        return [RowConvertOp(RCO.CONVERT_SUB_ENTITY, col_idx, column._rcos), _RCO_PUSH]

    def _rco_for_many_relation(self, Relation relation, dict existing=None):
        cdef EntityType load = relation._impl_.joined
        cdef Query q

        if isinstance(relation._impl_, ManyToMany):
            q = Query(relation._impl_.across) \
                .columns(relation._impl_.joined) \
                .join(relation._impl_.joined, relation._impl_.join_expr, "INNER") \
                .where(relation._impl_.across_join_expr)
        else:
            q = Query(load).where(relation._impl_.join_expr)

        if self.q._load:
            q._load = dict(self.q._load)

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
