import cython

cdef class EntityType(type):
    cdef readonly tuple __fields__
    cdef readonly tuple __field_names__
    cdef dict __meta__
    cdef object __weakref__


cdef class EntityBase:
    cdef readonly EntityState __state__


@cython.final
cdef class EntityState:
    cdef tuple fields
    cdef tuple data
    cdef tuple dirty
    cdef bint is_dirty

    cdef bint set_value(self, int index, object value)

    cdef object get_value(self, int index)

    cdef void del_value(self, int index)

    cpdef reset(self)
