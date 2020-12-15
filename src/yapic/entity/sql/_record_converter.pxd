import cython

from yapic.entity._field cimport StorageTypeFactory


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


# @cython.final
# class RecordConverter:
#     cdef list stack
#     cdef object result
#     cdef object tmp
#     cdef RCState state
#     cdef EntityState entity_state
#     cdef StorageTypeFactory tf

#     cdef set_attr_from_record(self, RowConvertOp rco, object record)
