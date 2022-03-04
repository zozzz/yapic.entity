import cython
from cpython.object cimport PyObject

from ._expression cimport AliasExpression, Expression
from ._registry cimport Registry
from ._resolve cimport ResolveContext
# from ._relation cimport Relation


cdef class NOTSET:
    pass


# TODO: _stage_resolving call resolve only when not resolved
# ezt használni ahelyett, hogy van-e deferred field

ctypedef enum EntityStage:
    UNRESOLVED = 1
    RESOLVING = 2
    RESOLVED = 3
    # INITED = 4


# TODO: remove __deferred__ list, but provide abality, to query deferred fields


cdef class EntityType(type):
    cdef readonly tuple __attrs__
    cdef readonly tuple __fields__
    cdef readonly tuple __props__
    cdef readonly tuple __pk__
    # cdef readonly list __deferred__
    cdef public list __fix_entries__
    cdef public list __triggers__
    cdef readonly dict __extgroups__
    cdef readonly EntityDependency __deps__
    cdef readonly Polymorph __polymorph__

    cdef ResolveContext resolve_ctx
    cdef EntityStage stage
    cdef PyObject* registry_ref
    cdef PyObject* meta

    cdef EntityType get_base_entity(self)
    cdef Registry get_registry(self)
    cdef list _compute_attrs(self, EntityType base_entity, PolymorphDict polymorph_dict, object cls_dict)
    cdef list _compute_triggers(self)
    # cdef object _finalize(self)
    cdef object _stage_resolving(self)
    cdef object _stage_resolved(self)
    cdef bint is_deferred(self)
    cdef bint is_resolved(self)
    cdef bint is_empty(self)
    cdef object _has_attr(self, str key)

    cpdef object __entity_ready__(self)
    cpdef object get_meta(self, str key=*, default=*)
    cpdef object set_meta(self, str key, object value)
    cpdef bint has_meta(self, str key)


@cython.final
cdef class EntityAlias(EntityType):
    cdef object entity_ref

    cdef EntityType get_entity(self)
    cdef EntityType set_entity(self, EntityType entity)
    cdef void _copy_meta(self, keys)
    cdef EntityType _clone_poly_parents(self, Polymorph poly, EntityType aliased)


cpdef bint is_entity_alias(object o)
cpdef EntityType get_alias_target(EntityType o)


cdef class EntityBase:
    cdef readonly EntityState __state__
    cdef int iter_index


cdef class EntityAttribute(Expression):
    cdef object __weakref__
    cdef object _impl
    cdef object entity_ref
    cdef EntityStage stage

    cdef readonly str _key_
    cdef readonly int _index_
    cdef readonly str _name_
    cdef readonly object _default_
    cdef readonly EntityAttributeImpl _impl_
    # cdef readonly EntityType _entity_
    cdef readonly list _exts_
    cdef readonly EntityDependency _deps_
    cdef readonly bint _virtual_
    cdef readonly int _uid_

    cdef EntityType get_entity(self)
    # cdef EntityType set_entity(self, EntityType entity)

    cdef object _bind(self, object entity_ref, object registry_ref)
    cdef EntityAttribute _rebind(self, EntityType entity)
    cdef object _stage_resolving(self, ResolveContext ctx)
    cdef object _resolve_deferred(self, ResolveContext ctx)
    cdef object _stage_resolved(self)

    cpdef object init(self)
    cpdef clone(self)
    cpdef get_ext(self, ext_type)
    cpdef clone_exts(self, EntityAttribute attr)
    cpdef copy_into(self, EntityAttribute other)
    cpdef _entity_repr(self)


cdef class EntityAttributeExt:
    cdef object attr_ref
    cdef EntityStage stage

    cdef object _bind(self, object attr_ref)
    cdef object _stage_resolving(self, ResolveContext ctx)
    cdef object _resolve_deferred(self, ResolveContext ctx)
    cdef object _stage_resolved(self)

    cdef EntityAttribute get_attr(self)
    cdef EntityType get_entity(self)
    # cdef object set_attr(self, EntityAttribute val)

    cpdef object init(self)
    cpdef object clone(self)
    cpdef object add_to_group(self, str key)


@cython.final
cdef class EntityAttributeExtList(list):
    pass


cdef class EntityAttributeExtGroup:
    cdef readonly str name
    cdef readonly list items
    cdef readonly object type


cdef class EntityAttributeImpl:
    cdef EntityStage stage


    cdef object _stage_resolving(self, ResolveContext ctx, EntityAttribute attr)
    cdef object _resolve_deferred(self, ResolveContext ctx, EntityAttribute attr)
    cdef object _stage_resolved(self, EntityAttribute attr)

    # returns true when successfully bind, otherwise the system can try bind in the later time
    cpdef object init(self, EntityAttribute attr)
    cpdef object clone(self)
    cpdef object getattr(self, EntityAttribute attr, object key)
    cpdef object getitem(self, EntityAttribute attr, object index)
    # cpdef object entity_ready(self, EntityAttribute attr)

    cdef object state_init(self, object initial)
    cdef object state_set(self, object initial, object current, object value)
    cdef object state_get_dirty(self, object initial, object current)
    cdef bint _is_eq(self, object other)


@cython.final
@cython.freelist(1000)
cdef class EntityState:
    cdef EntityType entity
    cdef tuple initial
    cdef tuple current
    cdef int field_count
    cdef readonly bint exists

    # @staticmethod
    # cdef EntityState create_from_dict(EntityType entity, dict data)

    cdef object init(self)
    cpdef object update(self, dict data, bint is_initial=*)

    cdef object set_value(self, EntityAttribute attr, object value)
    cdef object set_initial_value(self, EntityAttribute attr, object value)
    cdef object get_value(self, EntityAttribute attr)
    cdef object del_value(self, EntityAttribute attr)

    cdef list data_for_insert(self)
    cdef list data_for_update(self)

    cdef object attr_changes(self, EntityAttribute attr)

    cdef reset_all(self)
    cdef reset_attr(self, EntityAttribute attr)
    cdef bint is_eq_reflected(self, EntityState other)
    cdef bint _is_empty(self)


@cython.final
cdef class EntityDependency:
    cdef object registry_ref
    cdef set entity_names

    cdef Registry get_registry(self)

    cpdef add_entity(self, EntityType entity)
    cpdef EntityDependency merge(self, EntityDependency other)
    cpdef EntityDependency intersection(self, EntityDependency other)
    cpdef list entities(self)
    cpdef EntityDependency clone(self)


@cython.final
cdef class DependencyList(list):
    cdef list items
    cdef dict circular

    cpdef add(self, EntityType item)
    cpdef index(self, EntityType item)
    cdef _add(self, EntityType entity, EntityType dep, set cd)
    cdef _resolve_circular(self, EntityType entity, EntityType dep, set cd)

    # cdef add_circular(self, EntityType entity, EntityType dep)
    # cdef _add(self, EntityType entity, set cd)


@cython.final
cdef class PolymorphDict(dict):
    cdef readonly tuple id_fields

    @staticmethod
    cdef tuple normalize_id(object id)

    cdef object add_entity(self, object poly_id, EntityType entity)
    cdef EntityType get_entity(self, object poly_id)
    cdef tuple get_id(self, EntityType entity)


@cython.final
cdef class Polymorph:
    # TODO: Relation type
    cdef readonly object parent
    cdef readonly PolymorphDict info
    cdef readonly tuple id_values
    cdef readonly list children
    cdef dict poly_ids

    cdef object add_child(self, object relation)
    cpdef list parents(self)
    cdef EntityType get_entity(self, object id)
    cdef tuple get_id(self, EntityType entity)


cdef inline entity_is_builtin(EntityType entity):
    try:
        return entity.__meta__["is_builtin"] is True
    except KeyError:
        return False


cdef inline entity_is_virtual(EntityType entity):
    try:
        return entity.__meta__["is_virtual"] is True
    except KeyError:
        return False


cdef inline entity_is_type(EntityType entity):
    try:
        return entity.__meta__["is_type"] is True
    except KeyError:
        return False
