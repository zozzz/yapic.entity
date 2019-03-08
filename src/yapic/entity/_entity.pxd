import cython

@cython.auto_pickle(False)
cdef class EntityType(type):
    cdef readonly tuple __fields__
    cdef readonly tuple __field_names__
    cdef dict __meta__
    cdef object __weakref__


@cython.auto_pickle(False)
cdef class EntityBase:
    cdef readonly EntityState __state__


@cython.auto_pickle(False)
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



# @cython.auto_pickle(False)
# cdef class EntityMeta:
#     cdef dict data


# @cython.auto_pickle(False)
# cdef class EntityState:
#     cdef tuple fields
#     cdef tuple data
#     cdef tuple dirty
