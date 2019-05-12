
cdef class RCState:
    cdef readonly dict cache
    cdef readonly object conn


# cdef class RecordConverter:
#     cdef readonly list operations
#     cdef readonly object result
#     cdef int current_idx
#     cdef list stack

#     cdef object begin(self)
#     cdef object next(self, object record, RCState state)
