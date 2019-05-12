import cython

from yapic.entity._entity cimport EntityType, PolymorphMeta, DependencyList, get_alias_target
from yapic.entity._field cimport Field
from yapic.entity._field_impl cimport CompositeImpl
from yapic.entity._expression cimport (Expression, AliasExpression, DirectionExpression, Visitor, BinaryExpression,
    UnaryExpression, CastExpression, CallExpression, RawExpression, PathExpression)
from yapic.entity._expression import and_
from yapic.entity._relation cimport Relation, RelationImpl, ManyToMany, RelatedField, determine_join_expr
from yapic.entity._error cimport JoinError


cdef class Query(Expression):
    def __cinit__(self):
        self._entities = []

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
                    or isinstance(col, RawExpression):
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
            if isinstance(item, DirectionExpression) or isinstance(item, RawExpression):
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

    def join(self, what, condition = None, type = "INNER"):
        cdef RelationImpl impl
        cdef EntityType joined

        if self._joins is None:
            self._joins = {}

        if isinstance(what, Relation):
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

            if entity in self._select_from:
                return

        elif isinstance(what, EntityType):
            if what in self._select_from:
                return

            entity = <EntityType>what

            if condition is None:
                condition = determine_join(self, entity)

        if entity not in self._entities:
            self._entities.append(entity)

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

    cpdef Query clone(self):
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

        return q

    cdef tuple finalize(self):
        cdef Query res = self.clone()
        cdef QueryFinalizer qf = QueryFinalizer(res)

        qf.finalize()

        return res, qf.rcos


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
        elif self.op == RCO.LOAD_ENTITY: name = "LOAD_ENTITY"
        elif self.op == RCO.SET_ATTR: name = "SET_ATTR"
        elif self.op == RCO.SET_ATTR_RECORD: name = "SET_ATTR_RECORD"
        elif self.op == RCO.GET_RECORD: name = "GET_RECORD"

        return "<RCO:%s %r %r>" % (name, self.param1, self.param2)


cdef class QueryFinalizer(Visitor):
    def __cinit__(self, Query q):
        self.q = q
        self.rcos = []

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
        self.q.join(expr._entity_)

    def visit_const(self, expr):
        pass

    def visit_query(self, expr):
        # TODO: ...
        pass

    def visit_alias(self, AliasExpression expr):
        self.visit(expr.expr)

    def visit_path(self, PathExpression expr):
        if isinstance(expr._primary_, Relation):
            self.q.join(expr._primary_)

        return PathExpression(expr._primary_, list(expr._path_))

    def finalize(self, *expr_list):
        if self.q._columns:     self._visit_columns(list(self.q._columns))
        if self.q._where:       self._visit_list(self.q._where)
        if self.q._order:       self._visit_list(self.q._order)
        if self.q._group:       self._visit_list(self.q._group)
        if self.q._having:      self._visit_list(self.q._having)
        if self.q._distinct:    self._visit_list(self.q._distinct)

        if not self.q._columns:
            self._visit_columns(self.q._select_from)

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
                self._select_entity(<EntityType>expr)
                continue
            elif isinstance(expr, PathExpression):
                path = <PathExpression>expr
                last_entry = path._path_[len(path._path_) - 1]

                if isinstance(last_entry, Field) and isinstance((<Field>last_entry)._impl_, CompositeImpl):
                    entity = (<CompositeImpl>(<Field>last_entry)._impl_)._entity_
                    primary_field = path._primary_
                    pstart = 0
                    if isinstance(primary_field, Relation):
                        primary_field = path._path_[0]
                        pstart = 1

                    _path = []
                    for i in range(pstart, len(path._path_)):
                        p = path._path_[i]
                        _path.append(p._key_)

                    self.rcos.append(self._rco_for_composite(primary_field, entity, _path))
                    self.visit(expr)
                    continue
            elif isinstance(expr, Field) and isinstance((<Field>expr)._impl_, CompositeImpl):
                self.rcos.append(self._rco_for_composite((<Field>expr), (<CompositeImpl>(<Field>expr)._impl_)._entity_, []))
                self.visit(expr)
                continue

            self.rcos.append([RowConvertOp(RCO.GET_RECORD, len(self.q._columns))])
            self.q._columns.append(expr)
            self.visit(expr)

    def _visit_list(self, expr_list):
        for expr in expr_list:
            self.visit(expr)

    def _select_entity(self, EntityType entity):
        cdef PolymorphMeta polymorph = entity.__meta__.get("polymorph", None)

        if polymorph:
            self._select_polymorph(entity, polymorph)
        else:
            self.rcos.append(self._rco_for_entity(entity, entity.__fields__))

    def _select_polymorph(self, EntityType entity, PolymorphMeta poly):
        cdef list parents = poly.parents(entity)
        # cdef EntityType ent
        cdef EntityType aliased
        cdef Relation relation
        cdef Relation parent_relation = None
        cdef Field field
        cdef list rco = []
        cdef dict fields = {}
        cdef dict create_poly = {}

        for relation in parents:
            self.q.join(relation, None, "INNER")

            if parent_relation:
                before_create = [
                    RowConvertOp(RCO.POP),
                    RowConvertOp(RCO.SET_ATTR, parent_relation),
                ]
            else:
                before_create = []
            parent_relation = relation

            rco.extend(self._rco_for_entity(relation._impl_.joined, relation._impl_.joined.__fields__, fields, before_create))
            rco.append(RowConvertOp(RCO.PUSH))

        if parent_relation:
            before_create = [
                RowConvertOp(RCO.POP),
                RowConvertOp(RCO.SET_ATTR, parent_relation),
            ]
        else:
            before_create = []

        aliased = get_alias_target(entity)
        rco.extend(self._rco_for_entity(entity, entity.__fields__, fields, before_create))

        if self._add_poly_child(create_poly, aliased, poly, fields):
            rco.append(RowConvertOp(RCO.PUSH))

            id_fields = []
            for ent_id in poly.id_fields:
                id_fields.append(fields[ent_id])

            rco.append(RowConvertOp(RCO.CREATE_POLYMORPH_ENTITY, tuple(id_fields), create_poly))

        self.rcos.append(rco)

    def _add_poly_child(self, dict create_poly, EntityType entity, PolymorphMeta poly, dict fields):
        cdef Relation relation
        cdef EntityType child
        cdef list rco = None

        for relation in poly.children(entity):
            child = relation._entity_
            self.q.join(child, relation._default_, "LEFT")

            rco = self._rco_for_entity(child, child.__fields__, fields, [
                RowConvertOp(RCO.POP),
                RowConvertOp(RCO.SET_ATTR, relation),
            ])
            rco.append(RowConvertOp(RCO.PUSH))
            create_poly[poly.entities[child][0]] = rco

            if not self._add_poly_child(create_poly, child, poly, fields):
                rco.pop()

        return rco is not None


    def _rco_for_entity(self, EntityType entity_type, fields, dict existing=None, list before_create=[]):
        if existing is None:
            existing = {}

        cdef EntityType aliased = get_alias_target(entity_type)
        cdef list rco = [RowConvertOp(RCO.CREATE_STATE, aliased)]
        cdef Field field

        for field in fields:
            if isinstance(field._impl_, CompositeImpl):
                rco[0:0] = self._rco_for_composite(field, (<CompositeImpl>field._impl_)._entity_)
                rco.append(RowConvertOp(RCO.POP))
                rco.append(RowConvertOp(RCO.SET_ATTR, field))
                # composite = (<CompositeImpl>field._impl_)._entity_
                # cfields = {}
                # for cfield in composite.__fields__:
                #     cfields[cfield._name_] = len(self.q._columns)
                #     self.q._columns.append(getattr(field, cfield._key_))

                # print(cfields)
                # rco[0:0] = self._rco_for_entity(composite, composite.__fields__, cfields) + [RowConvertOp(RCO.PUSH)]
                # rco.append(RowConvertOp(RCO.POP))
                # rco.append(RowConvertOp(RCO.SET_ATTR, field))
            else:
                try:
                    idx = existing[field._name_]
                except KeyError:
                    try:
                        idx = self._find_column_index(field)
                    except ValueError:
                        idx = len(self.q._columns)
                        self.q._columns.append(field)
                        existing[field._name_] = idx

                rco.append(RowConvertOp(RCO.SET_ATTR_RECORD, field, idx))

        rco.extend(before_create)
        rco.append(RowConvertOp(RCO.CREATE_ENTITY, aliased))
        return rco

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
                rco.append(RowConvertOp(RCO.POP))
                rco.append(RowConvertOp(RCO.SET_ATTR, f))
            else:
                idx = len(self.q._columns)
                self.q._columns.append(getattr(src, f._name_))
                rco.append(RowConvertOp(RCO.SET_ATTR_RECORD, f, idx))

        rco.append(RowConvertOp(RCO.CREATE_ENTITY, entity))
        rco.append(RowConvertOp(RCO.PUSH))
        return rco


    def _find_column_index(self, Field field):
        for i, c in enumerate(self.q._columns):
            if isinstance(c, Field) \
                    and (<Field>c)._entity_ is field._entity_ \
                    and (<Field>c)._name_ == field._name_:
                return i
        raise ValueError()


cdef inline determine_join(Query q, EntityType joined):
    cdef EntityType ent
    condition = None

    for ent in q._entities:
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

