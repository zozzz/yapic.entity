from ._record_converter cimport RCState


cdef class QueryContext:
    cdef readonly object conn
    cdef object cursor_factory
    cdef list rcos_list
    cdef RCState rc_state

    cdef convert_row(self, object row)
