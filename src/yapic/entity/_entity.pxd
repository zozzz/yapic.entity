import cython
from cpython.object cimport PyObject
from ._relation cimport RelationState

cdef class EntityType(type):
    cdef readonly tuple __fields__
    cdef readonly tuple __field_names__
    cdef readonly tuple __relations__
    cdef PyObject* meta
    cdef object __weakref__


cdef class EntityBase:
    cdef readonly FieldState __fstate__
    cdef readonly RelationState __rstate__


@cython.final
@cython.freelist(1000)
cdef class FieldState:
    cdef tuple fields
    cdef tuple data
    cdef tuple dirty
    cdef bint is_dirty

    cdef bint set_value(self, int index, object value)
    cdef object get_value(self, int index)
    cdef void del_value(self, int index)

    cpdef reset(self)

