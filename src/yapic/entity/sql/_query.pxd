import cython
from .._expression cimport Expression, Visitor
from .._entity cimport EntityType, EntityAttribute
from ._dialect cimport Dialect


@cython.final
cdef class Query(Expression):
    cdef readonly list _select_from
    cdef readonly list _columns
    cdef readonly list _where
    cdef readonly list _order
    cdef readonly list _group
    cdef readonly list _having
    cdef readonly list _distinct
    cdef readonly list _prefix
    cdef readonly list _suffix
    cdef readonly dict _joins
    cdef readonly dict _aliases
    cdef readonly slice _range
    cdef readonly QueryLock _lock
    cdef readonly set _entities
    cdef readonly set _reduce_children
    cdef readonly QueryLoad _load
    cdef readonly bint _as_row
    cdef readonly bint _as_json
    cdef readonly Query _parent
    cdef dict __expr_alias
    cdef int __alias_c
    cdef bint _allow_clone
    cdef list _rcos
    cdef list _pending_joins

    cpdef Query clone(self)
    cdef tuple finalize(self, QueryCompiler compiler)
    cdef str get_expr_alias(self, object expr)
    cdef EntityType _find_entity(self, EntityType entity, bint allow_parent)
    cdef str _get_next_alias(self)
    cdef object _resolve_pending_joins(self)
    # cdef _add_entity(self, EntityType ent)

ctypedef enum QUERY_LOCK_TYPE:
    UPDATE = 1
    NO_KEY_UPDATE = 2
    SHARE = 3
    KEY_SHARE = 4

ctypedef enum QUERY_LOCK_FALLBACK:
    WAIT = 1
    NOWAIT = 2
    SKIP_LOCKED = 3

@cython.final
cdef class QueryLock:
    cdef readonly QUERY_LOCK_TYPE type
    cdef readonly tuple refs
    cdef readonly QUERY_LOCK_FALLBACK fallback


ctypedef enum QLS:
    SKIP = 0
    EXPLICIT = 1
    IMPLICIT = 2
    ALWAYS = 4


@cython.final
cdef class QueryLoad(Visitor):
    cdef set entries
    cdef int in_explicit

    cdef object add(self, tuple input)
    cdef QLS get(self, EntityAttribute attr)
    cdef _add_entity_attr(self, EntityAttribute attr)
    cdef QueryLoad clone(self)


cdef class QueryFinalizer(Visitor):
    cdef readonly Query q
    cdef readonly list rcos
    cdef readonly int in_or
    cdef readonly QueryCompiler compiler
    cdef readonly dict virtual_indexes


cdef class QueryCompiler(Visitor):
    cdef readonly Dialect dialect
    cdef readonly Query query
    cdef readonly list rcos_list

    cpdef compile_select(self, Query query)
    cpdef compile_insert(self, EntityType entity, list attrs, list names, list values, bint inline_values=*)
    cpdef compile_insert_or_update(self, EntityType entity, list attrs, list names, list values, bint inline_values=*)
    cpdef compile_update(self, EntityType entity, list attrs, list names, list values, list where, bint inline_values=*)
    cpdef compile_delete(self, EntityType entity, list attrs, list names, list values, list where, bint inline_values=*)


"""
Example:
    * Load one entity::

        rco = [
            (CREATE_ENTITY, User),
            (SET_ATTR_FROM_RECORD, User.id, 0),
            (SET_ATTR_FROM_RECORD, User.name, 1),
            ...
        ]

    * Load polymorph entity::

        rco = [
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

        rco = [
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

ctypedef enum RCO:


    # Push previouse command result or any value into FIFO stack
    # (PUSH,)
    # (PUSH, AnyValue)
    PUSH = 1

    # Pop last item from FIFO stack
    # (POP,)
    POP = 2

    # Create entity state, and set state variable
    # (CREATE_STATE, EntityType)
    CREATE_STATE = 3

    # Create new entity instance from previously created state
    # returns entity
    # (CREATE_ENTITY, EntityType, none_if_empty=False)
    CREATE_ENTITY = 4

    # Create new entity instance or get from cache if exists, and change context to it
    # returns entity
    # (CREATE_ENTITY, EntityType, (record indexes for id fields))
    # CREATE_ENTITY_CACHED = 4

    # Create polymorph entity, and change context to it
    # (CREATE_POLYMORPH_ENTITY, (record_index_for_pks,), {polyid: jump_index})
    CREATE_POLYMORPH_ENTITY = 5

    # Convert first entity from array of records, and push into stack
    # (CONVERT_SUB_ENTITIES, sub_entities_row_idx, sub_entity_rcos)
    CONVERT_SUB_ENTITY = 6

    # Convert multiple entity from array of records, and push into stack
    # (CONVERT_SUB_ENTITIES, sub_entities_row_idx, sub_entity_rcos)
    CONVERT_SUB_ENTITIES = 7

    # Set attribute on current entity instance from previous command result
    # returns entity
    # (SET_ATTR, EntityAttribute)
    SET_ATTR = 8

    # Set attribute on current entity instance, from record
    # returns entity
    # (SET_ATTR_RECORD, EntityAttribute, record_index)
    SET_ATTR_RECORD = 9

    # Get value from record
    # (GET_RECORD, record_index)
    GET_RECORD = 10


@cython.final
@cython.freelist(1000)
cdef class RowConvertOp:
    cdef RCO op
    cdef object param1
    cdef object param2


@cython.final
cdef class QueryFactory:
    cdef readonly Query query
    cdef readonly tuple fields
    cdef readonly Expression join_expr
