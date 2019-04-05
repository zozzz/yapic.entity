from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF, Py_XDECREF, Py_XINCREF
from cpython.tuple cimport PyTuple_New, PyTuple_GET_ITEM, PyTuple_SET_ITEM


cdef class RowConverter:
    def __cinit__(self):
        self.actions = []

    cdef object convert(self, object row):
        cdef RCAction action
        cdef tuple entries


        length = len(self.actions)
        if length == 0:
            return row
        elif length == 1:
            action = <RCAction>self.actions[0]
            return action.convert(self.get_slice(row, action.start, action.stop))
        else:
            entries = PyTuple_New(length)
            for i, action in enumerate(self.actions):
                entry = action.convert(self.get_slice(row, action.start, action.stop))
                Py_INCREF(<object>entry)
                PyTuple_SET_ITEM(<object>entries, i, <object>entry)
            return entries


    cdef tuple get_slice(self, object row, int start, end):
        if isinstance(row, tuple):
            if start == 0 and end is None:
                return <tuple>row
            else:
                return (<tuple>row)[start:end]
        else:
            return self._get_slice(row, start, end)

    cdef tuple _get_slice(self, object row, int start, end):
        raise NotImplementedError()


cdef class RCAction:
    cdef object convert(self, tuple values):
        pass


cdef class RCTuple(RCAction):
    pass
