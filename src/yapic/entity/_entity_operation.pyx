from enum import IntFlag
from functools import cmp_to_key
from operator import __and__, __eq__, __neg__, __pos__

import cython

from ._entity cimport EntityBase, EntityType, EntityState, EntityAttribute, NOTSET, DependencyList
from ._relation cimport Relation, ManyToMany
from ._expression cimport Visitor, Expression, ConstExpression, RawExpression, UnaryExpression, BinaryExpression


class EntityOperation(IntFlag):
    REMOVE = 1
    UPDATE = 2
    CREATE = 4
    CREATE_OR_UPDATE = 8

    # (EntityAttribute target, EntityAttribute src)
    # XXX MUST HAVE LAST VALUE
    UPDATE_ATTR = 16


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
                a_idx = max(self.order.index(a_val[1]._entity_), self.order.index(a_val[2]._entity_))
                if b_op is EntityOperation.UPDATE_ATTR:
                    return a_idx - max(self.order.index(b_val[1]._entity_), self.order.index(b_val[2]._entity_))
                else:
                    return a_idx - self.order.index(type(b_val))
            else:
                a_idx = self.order.index(type(a_val))
                if b_op is EntityOperation.UPDATE_ATTR:
                    return a_idx - max(self.order.index(b_val[1]._entity_), self.order.index(b_val[2]._entity_))
                else:
                    return a_idx - self.order.index(type(b_val))


cpdef list collect_entity_operations(EntityBase entity):
    cdef DependencyList order = DependencyList()
    cdef list ops = []

    _collect_entities(entity, order, ops, determine_entity_op(entity))

    # print(order)
    # print("\n".join(map(repr, entitis)))

    ops.sort(key=cmp_to_key(_comparator(order)))

    # print("\n".join(map(repr, entitis)))

    return ops


cdef _collect_entities(EntityBase entity, DependencyList order, list ops, object op):
    cdef EntityState state = entity.__state__
    cdef EntityAttribute attr
    cdef list add
    cdef list rem
    cdef list chg
    cdef EntityBase related
    cdef bint is_dirty

    for attr, (add, rem, chg) in state.changed_realtions():
        is_dirty = True
        for related in add:
            set_related_attrs(<Relation>attr, entity, related, order, ops)
            _collect_entities(related, order, ops, determine_entity_op(related))

        for related in rem:
            del_related_attrs(<Relation>attr, entity, related, order, ops)
            _collect_entities(related, order, ops, determine_entity_op(related))

        for related in chg:
            set_related_attrs(<Relation>attr, entity, related, order, ops)
            _collect_entities(related, order, ops, determine_entity_op(related))

    if (is_dirty or state.is_dirty) and entity not in ops:
        ops.append((op, entity))
        order.add(type(entity))


cdef set_related_attrs(Relation attr, EntityBase main, EntityBase related, DependencyList order, list ops):
    if isinstance(attr._impl_, ManyToMany):
        across_entity = attr._impl_.across()

        append_fields(across_entity, main, attr._impl_.across_join_expr, ops)
        append_fields(across_entity, related, attr._impl_.join_expr, ops)

        _collect_entities(across_entity, order, ops, EntityOperation.CREATE_OR_UPDATE)
        order.add(type(across_entity))
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
        return EntityOperation.UPDATE
    else:
        return EntityOperation.CREATE


cdef class FieldUpdater(Visitor):
    cdef EntityBase target
    cdef EntityBase source
    cdef list result

    def __cinit__(self, EntityBase target, EntityBase source):
        self.target = target
        self.source = source
        self.result = []

    def visit_binary(self, BinaryExpression expr):
        left = self.visit(expr.left)
        right = self.visit(expr.right)

        if expr.op is __and__:
            self.visit(left)
            self.visit(right)
        elif expr.op is __eq__:
            if isinstance(left, EntityAttribute) and left._entity_ is type(self.target):
                if isinstance(right, Expression):
                    self.result.append((self.target, left, right))
                else:
                    self.target.__state__.set_value(left, right)
            elif isinstance(right, EntityAttribute) and right._entity_ is type(self.target):
                if isinstance(left, Expression):
                    self.result.append((self.target, right, left))
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
        if expr._entity_ is type(self.source):
            val = self.source.__state__.get_value(expr)
            if val is not NOTSET:
                return val
        return expr

    def visit_const(self, ConstExpression expr):
        return expr.value