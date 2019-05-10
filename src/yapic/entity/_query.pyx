from contextlib import contextmanager
from enum import Enum

from yapic.entity._entity cimport EntityType, PolymorphMeta, DependencyList, get_alias_target
from yapic.entity._field cimport Field
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
            what = impl.joined

            if what in self._select_from:
                return

        elif isinstance(what, EntityType):
            if what in self._select_from:
                return

            if condition is None:
                condition = determine_join(self, <EntityType>what)

        if what not in self._entities:
            self._entities.append(what)

        try:
            existing = self._joins[what]
        except KeyError:
            self._joins[what] = [what, condition, type]
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

    cdef Query finalize(self):
        cdef Query res = self.clone()
        cdef QueryFinalizer qf = QueryFinalizer(res)

        qf.finalize()

        return res

    # cdef _add_alias(self, source):
    #     if source not in self._aliases:
    #         if isinstance(source, EntityType):
    #             if (<EntityType>source).__meta__.get("is_alias", False) is True:
    #                 alias = (<EntityType>source).__name__
    #                 if alias:
    #                     self._aliases[source] = alias
    #                     return

    #         self._aliases[source] = f"t{len(self._aliases)}"



class RPK(Enum):
    """
    Example:
        * Load one entity::

            rpk = [
                (CREATE_ENTITY, User),
                (SET_ATTR_FROM_RECORD, User.id, 0),
                (SET_ATTR_FROM_RECORD, User.name, 1),
                ...
            ]

        * Load polymorph entity::

            rpk = [
                (CREATE_ENTITY, BaseEntity),
                (PUSH,),
                (SET_ATTR_FROM_RECORD, BaseEntity.id, 0),
                (SET_ATTR_FROM_RECORD, BaseEntity.variant, 1),
                (CREATE_ENTITY, ChildEntity),
                (SET_ATTR_FROM_RECORD, ChildEntity.field1, 2),
                (SET_ATTR_FROM_RECORD, ChildEntity.field2, 3),
                (POP,)
                (SET_ATTR, <Relation to baseEntity>),
            ]

        * Load joind relations::

            rpk = [
                (CREATE_ENTITY, User), // author
                (PUSH,)
                (SET_ATTR_FROM_RECORD, User.id, 2),
                (SET_ATTR_FROM_RECORD, User.name, 3),
                (CREATE_ENTITY, User), // updater
                (PUSH,)
                (SET_ATTR_FROM_RECORD, User.id, 4),
                (SET_ATTR_FROM_RECORD, User.name, 5),

                (CREATE_ENTITY, Article),
                (SET_ATTR_FROM_RECORD, Article.id, 0),
                (SET_ATTR_FROM_RECORD, Article.title, 1),
                (POP,)
                (SET_ATTR, Article.updater),
                (POP,)
                (SET_ATTR, Article.author),
            ]

    """

    # Push previous command result into stack, or any given value
    # (PUSH,)
    # (PUSH, AnyValue)
    PUSH = 1

    # Pop last item from stack
    POP = 2

    # Create new entity instance, and change context to it
    # (CREATE_ENTITY, EntityType)
    CREATE_ENTITY = 3

    # Switch context to this entity
    # (CHANGE_ENTITY, EntityBase)
    CHANGE_ENTITY = 4

    # Load entity from storage, and change to it
    # (LOAD_ENTITY, EntityType, condition)
    LOAD_ENTITY = 5

    # Set attribute on current entity instance, from record
    # (SET_ATTR_FROM_RECORD, EntityAttribute, record_index)
    SET_ATTR_FROM_RECORD = 6

    # Set attribute on current entity instance from previous command result
    # (SET_ATTR, EntityAttribute)
    SET_ATTR = 7

    # Copy value from record, into current result value
    # (VALUE_FROM_RECORD, record_index)
    VALUE_FROM_RECORD = 8


cdef class QueryFinalizer(Visitor):
    def __cinit__(self, Query q):
        self.q = q
        self.rpks = []

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

        print("="*40)
        for rpk in self.rpks:
            print("\n".join(map(repr, rpk)))
        print("="*40)

    def _visit_columns(self, expr_list):
        self.q._columns = []

        for expr in expr_list:
            if isinstance(expr, EntityType):
                self._select_entity(<EntityType>expr)
            else:
                self.rpks.append([(RPK.VALUE_FROM_RECORD, len(self.q._columns))])
                self.q._columns.append(expr)
                self.visit(expr)

    def _visit_list(self, expr_list):
        for expr in expr_list:
            self.visit(expr)

    def _select_entity(self, EntityType entity):
        cdef PolymorphMeta polymorph = entity.__meta__.get("polymorph", None)
        cdef DependencyList deps = DependencyList()
        cdef EntityType pentity
        cdef Field field
        cdef Relation relation
        cdef list rpk = []

        if polymorph:
            pcols = {}
            pentities = []
            self._get_poly_entities(entity, polymorph, pentities, deps)
            pentities.sort(key=deps.index)
            load_entitties = []

            for i in range(1, len(pentities)):
                pentity = pentities[i]
                relation = polymorph.entities[pentity][1]

                if relation.joined not in load_entitties:
                    load_entitties.append(relation.joined)

                self.q.join(relation, None, "LEFT")

            for pentity in load_entitties:
                for field in pentity.__fields__:
                    fname = field._name_
                    if fname not in pcols:
                        pcols[fname] = field

            print(load_entitties)

            pcol_list = list(pcols.values())
            for pentity in reversed(load_entitties):
                rpk.extend(self._rpk_for_entity(pentity, pcol_list))
                rpk.append((RPK.PUSH,))

            rpk.extend(self._rpk_for_entity(entity, entity.__fields__))

            for pentity in pentities:
                relation = polymorph.entities[pentity][1]
                rpk.append((RPK.POP,))
                rpk.append((RPK.SET_ATTR, relation))
        else:
            self.rpks.append(self._rpk_for_entity(entity, entity.__fields__))

    def _rpk_for_entity(self, EntityType entity_type, fields):
        res = [(RPK.CREATE_ENTITY, get_alias_target(entity_type))]

        for f in fields:
            if f._entity_ is entity_type:
                try:
                    idx = self._find_column_index(f)
                except ValueError:
                    idx = len(self.q._columns)
                    self.q._columns.append(f)

                res.append((RPK.SET_ATTR_FROM_RECORD, f, idx))

        return res

    def _find_column_index(self, Field field):
        for i, c in enumerate(self.q._columns):
            if isinstance(c, Field) \
                    and (<Field>c)._entity_ is field._entity_ \
                    and (<Field>c)._name_ == field._name_:
                return i
        raise ValueError()

    def _get_poly_entities(self, EntityType from_entity, PolymorphMeta polymorph, list result, DependencyList deps):
        cdef Relation relation

        if from_entity not in result:
            result.append(from_entity)
            deps.add(from_entity)

        for id, relation in polymorph.entities.values():
            if relation._impl_.joined is from_entity:
                self._get_poly_entities(relation._entity_, polymorph, result, deps)


    # @contextmanager
    # def _replace_visitor(self, name, v):
    #     original = getattr(self, name)
    #     try:
    #         setattr(self, name, v)
    #         yield self
    #     finally:
    #         setattr(self, name, original)


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

