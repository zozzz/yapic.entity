from enum import IntFlag
from functools import cmp_to_key
from operator import __and__, __eq__, __neg__, __pos__

import cython

from ._entity cimport EntityBase, EntityType, EntityState, EntityAttribute, NOTSET, DependencyList, get_alias_target, Polymorph
from ._relation cimport Relation, ManyToMany, OneToMany
from ._expression cimport Visitor, Expression, ConstExpression, RawExpression, UnaryExpression, BinaryExpression


class EntityOperation(IntFlag):
    REMOVE = 1
    UPDATE = 2
    INSERT = 4
    INSERT_OR_UPDATE = 8

    # (EntityBase target, EntityAttribute target_attr, EntityBase src, EntityAttribute src_attr)
    # XXX MUST HAVE LAST VALUE
    UPDATE_ATTR = 16


cpdef list save_operations(EntityBase entity):
    cdef DependencyList order = DependencyList()
    cdef list ops = []

    _collect_entities(entity, order, ops, determine_entity_op(entity))

    # print("\n".join(map(repr, ops)))
    # print(order)

    ops.sort(key=cmp_to_key(_comparator(order)))
    # import pprint; pprint.pprint(ops)
    return ops


cpdef list load_operations(EntityBase entity):
    pass


@cython.final
cdef class _comparator:
    cdef DependencyList order

    def __cinit__(self, DependencyList order):
        self.order = order

    def __call__(self, a, b):
        a_op, a_val = a
        b_op, b_val = b

        if a_op < EntityOperation.UPDATE_ATTR and b_op < EntityOperation.UPDATE_ATTR:
            return self.order.index(type(a_val)) - self.order.index(type(b_val))
        else:
            if a_op is EntityOperation.UPDATE_ATTR:
                a_idx = max(self.order.index(a_val[1]._entity_), self.order.index(a_val[3]._entity_))
                if b_op is EntityOperation.UPDATE_ATTR:
                    return a_idx - max(self.order.index(b_val[1]._entity_), self.order.index(b_val[3]._entity_))
                else:
                    return a_idx - self.order.index(type(b_val))
            else:
                a_idx = self.order.index(type(a_val))
                if b_op is EntityOperation.UPDATE_ATTR:
                    return a_idx - max(self.order.index(b_val[1]._entity_), self.order.index(b_val[3]._entity_))
                else:
                    return a_idx - self.order.index(type(b_val))


cdef _collect_entities(EntityBase entity, DependencyList order, list ops, object op):
    cdef EntityState state = entity.__state__
    cdef EntityAttribute attr
    cdef list add
    cdef list rem
    cdef list chg
    cdef EntityBase related
    cdef bint is_dirty

    set_poly_id(entity)

    for attr, (add, rem, chg) in state.changed_realtions():
        is_dirty = True
        for related in add:
            set_related_attrs(<Relation>attr, entity, related, order, ops)
            _collect_entities(related, order, ops, determine_entity_op(related))

        # TODO:
        # for related in rem:
        #     del_related_attrs(<Relation>attr, entity, related, order, ops)
        #     _collect_entities(related, order, ops, determine_entity_op(related))

        for related in chg:
            set_related_attrs(<Relation>attr, entity, related, order, ops)
            _collect_entities(related, order, ops, determine_entity_op(related))

    if (is_dirty or state.is_dirty or not state.exists) and entity not in ops:
        ops.append((op, entity))
        order.add(type(entity))


cdef set_poly_id(EntityBase main):
    cdef EntityType main_t = type(main)
    cdef Polymorph poly = main_t.__polymorph__
    cdef tuple pk = main.__pk__
    cdef EntityBase parent_entity
    cdef Relation parent

    if pk and poly is not None:
        parent_entity = main
        for parent in poly.parents():
            parent_entity = parent_entity.__state__.get_value(parent)
            parent_entity.__pk__ = pk


cdef set_related_attrs(Relation attr, EntityBase main, EntityBase related, DependencyList order, list ops):
    if isinstance(attr._impl_, ManyToMany):
        across_alias = (<ManyToMany>attr._impl_).get_across_alias()
        across_entity = get_alias_target(across_alias)()

        append_fields(across_entity, main, attr._impl_.across_join_expr, ops)
        append_fields(across_entity, related, attr._impl_.join_expr, ops)

        _collect_entities(across_entity, order, ops, EntityOperation.INSERT_OR_UPDATE)
        order.add(across_alias)
    elif isinstance(attr._impl_, OneToMany):
        append_fields(related, main, attr._impl_.join_expr, ops)
    else:
        append_fields(main, related, attr._impl_.join_expr, ops)


cdef del_related_attrs(Relation attr, EntityBase main, EntityBase related, DependencyList order, list ops):
    pass


cdef append_fields(EntityBase main, EntityBase related, Expression expr, list ops):
    v = FieldUpdater(main, related)
    v.visit(expr)

    for r in v.result:
        ops.append((EntityOperation.UPDATE_ATTR, r))


cdef determine_entity_op(EntityBase entity):
    if entity.__pk__:
        if entity.__state__.exists:
            return EntityOperation.UPDATE
        else:
            return EntityOperation.INSERT_OR_UPDATE
    else:
        return EntityOperation.INSERT


cdef class FieldUpdater(Visitor):
    cdef EntityBase target
    cdef EntityType target_t
    cdef EntityBase source
    cdef EntityType source_t
    cdef list result

    def __cinit__(self, EntityBase target, EntityBase source):
        self.target = target
        self.target_t = get_alias_target(type(target))
        self.source = source
        self.source_t = get_alias_target(type(source))
        self.result = []

    def visit_binary(self, BinaryExpression expr):
        left = self.visit(expr.left)
        right = self.visit(expr.right)

        if expr.op is __eq__:
            if isinstance(left, EntityAttribute):
                left = get_alias_target((<EntityAttribute>left).get_entity()).__attrs__[left._index_]

            if isinstance(right, EntityAttribute):
                right = get_alias_target((<EntityAttribute>right).get_entity()).__attrs__[right._index_]

            if isinstance(left, EntityAttribute) and test_entity_type_eq(self.target_t, (<EntityAttribute>left).get_entity()):
                if isinstance(right, EntityAttribute) and test_entity_type_eq(self.source_t, (<EntityAttribute>right).get_entity()):
                    self.result.append((self.target, left, self.source, right))
                else:
                    self.target.__state__.set_value(left, right)
            elif isinstance(right, EntityAttribute) and test_entity_type_eq(self.target_t, (<EntityAttribute>right).get_entity()):
                if isinstance(left, EntityAttribute) and test_entity_type_eq(self.source_t, (<EntityAttribute>left).get_entity()):
                    self.result.append((self.target, right, self.source, left))
                else:
                    self.target.__state__.set_value(right, left)
        else:
            raise ValueError("Unsupported operator: %r" % expr.op)

    def visit_unary(self, UnaryExpression expr):
        if expr.op is __neg__ or expr.op is __pos__:
            val = self.visit(expr.expr)
            if not isinstance(val, Expression):
                return expr.op(val)
            else:
                return expr
        else:
            raise ValueError("Unsupported operator: %r" % expr.op)

    # def visit_cast(self, CastExpression expr):
    #     return self.visit(expr.expr).cast(expr.type)

    def visit_raw(self, RawExpression expr):
        return expr

    def visit_field(self, EntityAttribute expr):
        if get_alias_target(expr.get_entity()) is self.source_t:
            val = self.source.__state__.get_value(expr)
            if val is not NOTSET:
                return val
        return expr

    def visit_const(self, ConstExpression expr):
        return expr.value


cdef bint test_entity_type_eq(EntityType a, EntityType b):
    if b.__polymorph__ is not None and issubclass(b, a):
        return True
    elif a.__polymorph__ is not None and issubclass(a, b):
        return True
    else:
        return a is b
