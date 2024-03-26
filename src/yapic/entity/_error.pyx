

cdef class EntityError(Exception):
    pass

cdef class JoinError(Exception):
    pass

cdef class MultipleRows(Exception):
    pass

cdef class MissingRow(Exception):
    pass
