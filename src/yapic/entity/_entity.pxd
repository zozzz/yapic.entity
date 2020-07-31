import cython
from cpython.object cimport PyObject

from ._expression cimport AliasExpression, Expression
from ._registry cimport Registry


cdef class NOTSET:
    pass


cdef class EntityType(type):
    cdef readonly tuple __attrs__
    cdef readonly tuple __fields__
    cdef readonly tuple __props__
    cdef readonly tuple __pk__
    cdef readonly list __deferred__
    cdef public list __fix_entries__
    cdef public list __triggers__
    cdef public tuple __extgroups__
    cdef PyObject* registry
    cdef PyObject* meta
    cdef readonly set __deps__
    cdef object __weakref__

    cdef object resolve_deferred(self)
    cpdef object __entity_ready__(self)


cpdef bint is_entity_alias(object o)
cpdef EntityType get_alias_target(EntityType o)


cdef class EntityBase:
    cdef readonly EntityState __state__
    cdef int iter_index


cdef class EntityAttribute(Expression):
    cdef object _impl
    cdef readonly str _key_
    cdef readonly int _index_
    cdef readonly str _name_
    cdef readonly object _default_
    cdef readonly EntityAttributeImpl _impl_
    cdef readonly EntityType _entity_
    cdef readonly list _exts_
    cdef readonly set _deps_
    cdef readonly bint _virtual_
    cdef readonly int _uid_

    cdef object init(self, EntityType entity)
    # returns true when successfully bind, otherwise the system can try bind in the later time
    cdef object bind(self)
    # cdef object entity_ready(self, EntityType entity)
    cpdef clone(self)
    cpdef get_ext(self, ext_type)
    cpdef clone_exts(self, EntityAttribute attr)
    cpdef copy_into(self, EntityAttribute other)


cdef class EntityAttributeExt:
    cdef readonly EntityAttribute attr
    cdef list _tmp
    cdef bint bound
    cdef str group_by

    cpdef object init(self, EntityAttribute attr)
    # returns true when successfully bind, otherwise the system can try bind in the later time
    cpdef object bind(self)
    cpdef object clone(self)
    # cpdef object entity_ready(self, EntityType entity)


cdef class EntityAttributeExtGroup:
    cdef readonly str name
    cdef readonly tuple items
    cdef readonly object type


cdef class EntityAttributeImpl:
    cdef bint inited

    # returns true when successfully bind, otherwise the system can try bind in the later time
    cpdef object init(self, EntityAttribute attr)
    cpdef object clone(self)
    cpdef object getattr(self, EntityAttribute attr, object key)
    cpdef object getitem(self, EntityAttribute attr, object index)
    # cpdef object entity_ready(self, EntityAttribute attr)

    cdef object state_init(self, object initial)
    cdef object state_set(self, object initial, object current, object value)
    cdef object state_get_dirty(self, object initial, object current)


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



@cython.final
cdef class DependencyList(list):
    cdef dict circular

    cpdef add(self, EntityType item)
    cdef _add(self, EntityType entity, EntityType dep, set cd)
    cdef _resolve_circular(self, EntityType entity, EntityType dep, set cd)

    # cdef add_circular(self, EntityType entity, EntityType dep)
    # cdef _add(self, EntityType entity, set cd)


@cython.final
cdef class PolymorphMeta:
    cdef readonly tuple id_fields
    cdef readonly dict entities

    @staticmethod
    cdef tuple normalize_id(object id)

    cdef object add(self, object id, EntityType entity, object relation)
    cpdef list parents(self, EntityType entity)
    cpdef list children(self, EntityType entity)
    cdef object _parents(self, EntityType entity, list result)
