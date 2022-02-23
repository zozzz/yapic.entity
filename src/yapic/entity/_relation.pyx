import sys
import cython
from cpython.ref cimport Py_XDECREF, Py_XINCREF, Py_DECREF, Py_INCREF, Py_CLEAR
from cpython.object cimport PyObject, PyObject_RichCompareBool, Py_EQ
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack
from cpython.module cimport PyImport_Import, PyModule_GetDict
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject

from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeImpl, EntityAttributeExt, EntityAttributeExtGroup, get_alias_target, NOTSET
from ._expression cimport Expression, Visitor, PathExpression, ExpressionPlaceholder
from ._field cimport Field, ForeignKey
from ._factory cimport Factory, ForwardDecl, new_instance_from_forward, is_forward_decl
from ._visitors cimport replace_entity, replace_placeholder
from ._error cimport JoinError
from ._resolve cimport ResolveContext


cdef class Relation(EntityAttribute):
    def __cinit__(self, *args, join = None):
        self._default_ = join

    cdef object _resolve_deferred(self, ResolveContext ctx):
        if EntityAttribute._resolve_deferred(self, ctx) is False:
            return False
        if not isinstance(self._impl_, RelationImpl):
            raise ValueError(f"Relation attribute implementation is wrong type: {self._impl_}")

        (<RelationImpl>self._impl_).set_relation(self)
        return True

    def __getattr__(self, name):
        if self.get_entity().is_deferred():
            raise RuntimeError(f"{self.get_entity()} is in deferred state something went wrong")

        cdef RelationImpl impl = self._impl_
        cdef EntityType joined = impl.get_joined_alias()
        cdef Expression expr = getattr(joined, name)

        return PathExpression([self, expr])

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation(self)

    def __repr__(self):
        return "<Relation %s :: %s>" % (self._entity_repr(), self._impl_)

    cpdef object clone(self):
        cdef Relation res = type(self)(self._impl_.clone())
        res._exts_ = self.clone_exts(res)
        res._default_ = self._default_
        res._key_ = self._key_
        res._name_ = self._name_
        res._deps_ = self._deps_.clone()
        return res


cdef class RelationImpl(EntityAttributeImpl):
    def __cinit__(self, joined, state_impl, *args):
        self.joined_entity_ref = <object>PyWeakref_NewRef(joined, None)
        self.state_impl = state_impl

    cdef EntityType get_joined_entity(self):
        return <object>PyWeakref_GetObject(self.joined_entity_ref)

    cdef EntityType get_joined_alias(self):
        if self.joined_alias_ref is None:
            original = self.get_joined_entity()
            self.joined_alias_ref = original.alias()
        return self.joined_alias_ref

    cdef object set_joined_alias(self, EntityType entity):
        self.joined_alias_ref = entity
        self._join_expr = None

    @property
    def joined(self):
        return self.get_joined_alias()

    @property
    def join_expr(self):
        return self.get_join_expr()

    @join_expr.setter
    def join_expr(self, Expression value):
        self._join_expr = value

    cdef Expression get_join_expr(self):
        raise NotImplementedError()

    cdef Relation set_relation(self, Relation relation):
        if self.relation_ref is None:
            self.relation_ref = <object>PyWeakref_NewRef(relation, None)
        else:
            current = <object>PyWeakref_GetObject(self.relation_ref)
            if current is not relation:
                raise RuntimeError(f"Can't rebind {self}")

    cdef Relation get_relation(self):
        if self.relation_ref is not None:
            return <object>PyWeakref_GetObject(self.relation_ref)
        else:
            return None

    cdef object _resolve_deferred(self, ResolveContext ctx, EntityAttribute relation):
        if EntityAttributeImpl._resolve_deferred(self, ctx, relation) is True:
            if relation._default_:
                # TODO: rework
                return self.resolve_default(ctx, relation)
            else:
                return True
        else:
            return False

    cdef object resolve_default(self, ResolveContext ctx, EntityAttribute relation):
        raise NotImplementedError()

    cpdef object clone(self):
        cdef RelationImpl c = type(self)(self.get_joined_entity(), self.state_impl)
        return c

    cdef object state_init(self, object initial):
        return self.state_impl.state_init(initial)

    cdef object state_set(self, object initial, object current, object value):
        return self.state_impl.state_set(initial, current, value)

    cdef object state_get_dirty(self, object initial, object current):
        return self.state_impl.state_get_dirty(initial, current)


cdef class ManyToOne(RelationImpl):
    def __repr__(self):
        if self.joined_alias_ref is not None:
            return f"ManyToOne {self.joined_alias_ref}"
        else:
            return f"ManyToOne {self.get_joined_entity()}"

    cdef Expression get_join_expr(self):
        if self._join_expr is None:
            self._join_expr = self._determine_join_expr(self.get_relation())
        return self._join_expr

    cdef Expression _determine_join_expr(self, EntityAttribute relation):
        cdef EntityType target = relation.get_entity()
        cdef EntityType joined = self.get_joined_entity()

        aliased = self.get_joined_alias()

        if relation._default_ is not None:
            join_expr = relation._default_
            join_expr = replace_entity(join_expr, joined, aliased)

            joined_aliased = get_alias_target(joined)
            if joined_aliased is not joined:
                join_expr = replace_entity(join_expr, joined_aliased, aliased)

            target_aliased = get_alias_target(target)
            if target_aliased is not target:
                join_expr = replace_entity(join_expr, target_aliased, target)

            join_expr = replace_placeholder(join_expr, {
                "_self_": target,
                "_joined_": aliased,
            })
        else:
            join_expr = determine_join_expr(target, aliased)

        return join_expr

    cdef object resolve_default(self, ResolveContext ctx, EntityAttribute relation):
        if isinstance(relation._default_, str):
            try:
                relation._default_ = ctx.eval(relation._default_, {
                    "_self_": ExpressionPlaceholder(relation.get_entity(), "_self_") ,
                    "_joined_": ExpressionPlaceholder(self.get_joined_entity(), "_joined_"),
                })
            except NameError:
                return False
        elif isinstance(relation._default_, Expression):
            return True
        elif callable(relation._default_):
            relation._default_ = relation._default_(relation.get_entity())
        else:
            raise ValueError("Invalid value for join expression: %r" % relation._default_)


cdef class OneToMany(RelationImpl):
    def __repr__(self):
        if self.joined_alias_ref is not None:
            return f"OneToMany {self.joined_alias_ref}"
        else:
            return f"OneToMany {self.get_joined_entity()}"

    cdef Expression get_join_expr(self):
        if self._join_expr is None:
            self._join_expr = self._determine_join_expr(self.get_relation())
        return self._join_expr

    cdef Expression _determine_join_expr(self, EntityAttribute relation):
        cdef EntityType target = relation.get_entity()
        cdef EntityType joined = self.get_joined_entity()

        aliased = self.get_joined_alias()

        if relation._default_:
            join_expr = relation._default_
            join_expr = replace_entity(join_expr, joined, aliased)

            joined_aliased = get_alias_target(joined)
            if joined_aliased is not joined:
                join_expr = replace_entity(join_expr, joined_aliased, aliased)

            target_aliased = get_alias_target(target)
            if target_aliased is not target:
                join_expr = replace_entity(join_expr, target_aliased, target)

            join_expr = replace_placeholder(join_expr, {
                "_self_": target,
                "_joined_": aliased,
            })
        else:
            join_expr = determine_join_expr(aliased, target)

        return join_expr

    cdef object resolve_default(self, ResolveContext ctx, EntityAttribute relation):
        if isinstance(relation._default_, str):
            try:
                relation._default_ = ctx.eval(relation._default_, {
                    "_self_": ExpressionPlaceholder(relation.get_entity(), "_self_") ,
                    "_joined_": ExpressionPlaceholder(self.get_joined_entity(), "_joined_"),
                })
            except NameError:
                return False
        elif callable(relation._default_) and not isinstance(relation._default_, Expression):
            relation._default_ = relation._default_(relation.get_entity())

        if isinstance(relation._default_, Expression):
            return True
        else:
            raise ValueError("Invalid value for join expression: %r" % relation._default_)


cdef class ManyToMany(RelationImpl):
    def __cinit__(self, joined, state_impl, across):
        self.across_entity_ref = <object>PyWeakref_NewRef(across, None)

    @property
    def across(self):
        return self.get_across_alias()

    @property
    def across_join_expr(self):
        return self.get_across_join_expr()

    @across_join_expr.setter
    def across_join_expr(self, Expression value):
        self._across_join_expr = value

    cdef object set_joined_alias(self, EntityType entity):
        self.joined_alias_ref = entity
        self._across_join_expr = None
        self._join_expr = None

    cdef object set_across_alias(self, EntityType entity):
        self.across_alias_ref = entity
        self._across_join_expr = None
        self._join_expr = None

    cdef EntityType get_across_entity(self):
        return <object>PyWeakref_GetObject(self.across_entity_ref)

    cdef EntityType get_across_alias(self):
        if self.across_alias_ref is None:
            original = self.get_across_entity()
            self.across_alias_ref = original.alias()
        return self.across_alias_ref

    cdef Expression get_join_expr(self):
        if self._join_expr is None:
            self._across_join_expr, self._join_expr = self._determine_join_expr(self.get_relation())
        return self._join_expr

    cdef Expression get_across_join_expr(self):
        if self._across_join_expr is None:
            self._across_join_expr, self._join_expr = self._determine_join_expr(self.get_relation())
        return self._across_join_expr

    cdef tuple _determine_join_expr(self, EntityAttribute relation):
        cdef EntityType target = relation.get_entity()
        cdef EntityType joined = self.get_joined_entity()
        cdef EntityType across = self.get_across_entity()

        joined_alias = self.get_joined_alias()
        across_alias = self.get_across_alias()

        if relation._default_:
            across_join_expr = relation._default_[across]
            across_join_expr = replace_entity(across_join_expr, across, across_alias)
            across_join_expr = replace_entity(across_join_expr, joined, joined_alias)
            join_expr = relation._default_[joined]
            join_expr = replace_entity(join_expr, across, across_alias)
            join_expr = replace_entity(join_expr, joined, joined_alias)

            target_aliased = get_alias_target(target)
            if target_aliased is not target:
                across_join_expr = replace_entity(across_join_expr, target_aliased, target)
                join_expr = replace_entity(join_expr, target_aliased, target)
        else:
            across_join_expr = determine_join_expr(across_alias, target)
            join_expr = determine_join_expr(across_alias, joined_alias)

        return (across_join_expr, join_expr)

    cdef object resolve_default(self, ResolveContext ctx, EntityAttribute relation):
        if isinstance(relation._default_, dict):
            resolved = {}
            for entity, join in relation._default_.items():
                if isinstance(entity, str):
                    try:
                        entity = ctx.eval(entity, {})
                    except NameError:
                        return False

                if not isinstance(entity, EntityType):
                    raise ValueError("Invalid value for join expression: %r" % relation._default_)

                if isinstance(join, str):
                    try:
                        join = ctx.eval(join, {})
                    except NameError:
                        return False

                if not isinstance(join, Expression):
                    raise ValueError("Invalid value for join expression: %r" % relation._default_)

                resolved[entity] = join
            relation._default_ = resolved
            return True
        else:
            raise ValueError("Invalid value for join expression: %r" % relation._default_)

    cpdef object clone(self):
        cdef ManyToMany c = type(self)(self.get_joined_entity(), self.state_impl, self.get_across_entity())
        return c

    def __repr__(self):
        return "ManyToMany %r => %r" % (self.get_across_entity(), self.get_joined_entity())


cdef determine_join_expr(EntityType entity, EntityType joined):
    if not entity.__extgroups__:
        raise JoinError("Can't determine join condition between %s <-> %s" % (entity, joined))

    cdef tuple keys
    cdef Field field
    cdef ForeignKey fk
    cdef object found = None
    cdef EntityAttributeExtGroup group

    for group in entity.__extgroups__.values():
        if group.type is not ForeignKey:
            continue

        fk = <ForeignKey>group.items[0]

        if fk.ref.get_entity() is joined:
            if found is not None:
                raise JoinError("Multiple join conditions between %s <-> %s" % (entity, joined))

            found = fk.attr == fk.ref
            for i in range(1, len(group.items)):
                fk = <ForeignKey>group.items[i]
                found &= fk.attr == fk.ref

    if found is None:
        aliased_ent = get_alias_target(entity)
        aliased_join = get_alias_target(joined)

        if aliased_ent is not entity or aliased_join is not joined:
            found = determine_join_expr(aliased_ent, aliased_join)
            if aliased_ent is not entity:
                found = replace_entity(found, aliased_ent, entity)
            if aliased_join is not joined:
                found = replace_entity(found, aliased_join, joined)
        else:
            raise JoinError("Can't determine join condition between %s <-> %s" % (entity, joined))

    return found


# ****************************************************************************
# ** VALUE STORES **
# ****************************************************************************


cdef class ValueStore:
    cpdef object state_init(self, object initial):
        raise NotImplementedError()

    cpdef object state_set(self, object initial, object current, object value):
        raise NotImplementedError()

    cpdef object state_get_dirty(self, object initial, object current):
        raise NotImplementedError()


cdef class RelatedItem(ValueStore):
    cpdef object state_init(self, object initial):
        return NOTSET

    cpdef object state_set(self, object initial, object current, object value):
        if value is not None and not isinstance(value, EntityBase):
            raise TypeError("Can't set attribute with this value: %r" % value)
        return value

    cpdef object state_get_dirty(self, object initial, object current):
        if initial is NOTSET or initial is None:
            if current is NOTSET or current is None:
                return NOTSET
            else:
                return ([current], [], [])
        else:
            if current is None:
                return ([], [current], [])
            elif initial == current:
                if current.__state__.is_dirty:
                    return ([], [], [current])
                else:
                    return NOTSET
            elif current is NOTSET:
                if initial.__state__.is_dirty:
                    return ([], [initial], [])
                else:
                    return NOTSET
            else:
                return ([current], [initial], [])


cdef class RelatedList(ValueStore):
    cpdef object state_init(self, object initial):
        if initial is NOTSET:
            return []
        else:
            return list(initial)

    cpdef object state_set(self, object initial, object current, object value):
        if value is None or isinstance(value, list):
            return value
        else:
            raise ValueError("Related list value must be list or None")

    cpdef object state_get_dirty(self, object initial, object current):
        if current is NOTSET:
            return NOTSET

        cdef list add = []
        cdef list rem = []
        cdef list chg = []
        cdef EntityBase ent

        if initial is NOTSET:
            add.extend(current)
        else:
            for ent in current:
                if ent in initial:
                    if ent.__state__.is_dirty:
                        chg.append(ent)
                else:
                    add.append(ent)

            for ent in initial:
                if ent not in current:
                    rem.append(ent)

        if add or rem or chg:
            return (add, rem, chg)
        else:
            return NOTSET


cdef class RelatedDict(ValueStore):
    pass


cdef class Loading(EntityAttributeExt):
    def __cinit__(self, *, bint always=False, list fields=None):
        self.always = always
        self.fields = fields

    cpdef clone(self):
        return Loading(always=self.always, fields=self.fields)

    def __repr__(self):
        return "@Loading(always=%s, fields=%s)" % (self.always, self.fields)


@cython.final
cdef class RelatedAttribute(EntityAttribute):
    def __cinit__(self, Relation rel, *, str name, **kwargs):
        self.__relation__ = rel
        self._name_ = name
        self._impl_ = RelatedAttributeImpl()
        self._virtual_ = True

    @property
    def __rattr__(self):
        if self.rattr is None:
            self.rattr = getattr((<ManyToOne>self.__relation__._impl_).get_joined_alias(), self._name_)
        return self.rattr

    @property
    def __rpath__(self):
        if self.rpath is None:
            self.rpath = getattr(self.__relation__, self._name_)
        return self.rpath

    # def __getattribute__(self, key):
    #     if key in ("__repr__", "_virtual_", "clone", "bind", "visit"):
    #         return object.__getattribute__(self, key)
    #     else:
    #         return getattr(self.__rattr__, key)
    def __getattr__(self, key):
        return getattr(self.__rattr__, key)

    def __setattr__(self, name, value):
        setattr(self.__rattr__, name, value)

    def __getitem__(self, key):
        return self.__rpath__[key]

    def __get__(self, instance, owner):
        if instance is None:
            return self
        elif isinstance(instance, EntityBase):
            return getattr((<EntityBase>instance).__state__.get_value(self.__relation__), self._name_)
        else:
            raise RuntimeError("...")

    def __set__(self, EntityBase instance, value):
        related = instance.__state__.get_value(self.__relation__)
        if related is NOTSET:
            related = (<RelationImpl>self.__relation__._impl_).get_joined_entity()
            related = get_alias_target(related)()
            instance.__state__.set_value(self.__relation__, related)
        setattr(related, self._name_, value)

    def __delete__(self, EntityBase instance):
        cdef EntityBase related = instance.__state__.get_value(self.__relation__)
        if related is not None:
            delattr(related, self._name_)

    def __repr__(self):
        return f"<RelatedAttribute {self._name_} -> {self.__relation__}>"

    cpdef clone(self):
        raise RuntimeError("Clone behavior is undefined, need manual clone")

    cdef object _resolve_deferred(self, ResolveContext ctx):
        if EntityAttribute._resolve_deferred(self, ctx) is True:
            return self.__relation__._stage_resolving(ctx)
        else:
            return False

    cpdef object init(self):
        if not isinstance(self.__relation__._impl_, ManyToOne):
            raise ValueError("RelatedAttribute only accepts ManyToOne type ralations")

        return EntityAttribute.init(self)

    cpdef visit(self, Visitor visitor):
        return self.__rpath__.visit(visitor)


cdef class RelatedAttributeImpl(EntityAttributeImpl):
    cdef object state_init(self, object initial):
        return NOTSET

    cdef object state_set(self, object initial, object current, object value):
        return NOTSET

    cdef object state_get_dirty(self, object initial, object current):
        return NOTSET
