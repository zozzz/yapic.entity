import cython


@cython.final
cdef class RowConverter:
    cdef list actions

    cdef object convert(self, object row)
    cdef tuple get_slice(self, object row, int start, end)
    cdef tuple _get_slice(self, object row, int start, end)


cdef class RCAction:
    cdef int start
    cdef int stop
    cdef object converter

    cdef object convert(self, tuple values)


cdef class RCTuple(RCAction):
    cdef tuple actions

