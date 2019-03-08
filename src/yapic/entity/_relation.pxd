import cython

from ._entity cimport EntityType
from ._expression cimport Expression
from ._field cimport Field


@cython.auto_pickle(False)
cdef class Relation(Expression):
    cdef readonly object __impl__
    cdef readonly EntityType __entity__
    cdef readonly EntityType __related__
    cdef readonly EntityType __across__


@cython.auto_pickle(False)
cdef class RelationField(Expression):
    cdef Relation relation
    cdef Field field


@cython.auto_pickle(False)
cdef class RelationImpl:
    cdef readonly object state


@cython.auto_pickle(False)
cdef class ManyToOne(RelationImpl):
    pass


@cython.auto_pickle(False)
cdef class OneToMany(RelationImpl):
    pass


@cython.auto_pickle(False)
cdef class ManyToMany(RelationImpl):
    pass
