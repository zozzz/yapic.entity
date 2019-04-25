from collections.abc import ItemsView

import cython
from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF, Py_XDECREF, Py_XINCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM, PyTuple_GET_ITEM
# from cpython.list cimport PyTuple_New, PyList_GET_ITEM

from ._entity cimport EntityBase, EntityAttribute, EntityState, NOTSET


@cython.final
@cython.freelist(200)
cdef class SerializerCtx:
    cdef bint skip_attribute(self, EntityAttribute attr, object value):
        if attr.get_ext(SkipSerialization):
            return True
        return value is NOTSET

    cdef SerializerCtx enter(self, str path):
        return self


@cython.final
@cython.freelist(200)
cdef class EntitySerializer:
    def __cinit__(self, EntityBase instance, SerializerCtx ctx):
        self.instance = instance
        self.entity = type(instance)
        self.ctx = ctx
        self.length = len(self.entity.__attrs__)

    def __iter__(self):
        self.idx = 0
        return self

    def __next__(self):
        cdef PyObject* attr
        cdef PyObject* attrs = <PyObject*>self.entity.__attrs__
        cdef EntityState state = <EntityState>self.instance.__state__

        if self.idx < self.length:
            attr = PyTuple_GET_ITEM(<object>attrs, self.idx)
            value = state.get_value(<EntityAttribute>(<object>attr))
            self.idx += 1

            if self.ctx.skip_attribute(<EntityAttribute>(<object>attr), value):
                return self.__next__()
            else:
                return ((<EntityAttribute>attr)._key_, create_serializable(value, self.ctx.enter((<EntityAttribute>attr)._key_)))
        else:
            raise StopIteration()


cdef inline object create_serializable(object value, SerializerCtx ctx):
    if isinstance(value, dict):
        return MappingGenerator(value, ctx)
    elif isinstance(value, (list, tuple)):
        return SequenceGenerator(value, ctx)
    else:
        return value


@cython.final
@cython.freelist(200)
cdef class MappingGenerator:
    cdef object iterable
    cdef object iterator
    cdef SerializerCtx ctx

    def __cinit__(self, iterable, SerializerCtx ctx):
        self.iterable = iterable
        self.ctx = ctx

    def __iter__(self):
        self.iterator = self.iterable.items()
        return self

    def __next__(self):
        key, value = next(self.iterator)
        return key, create_serializable(value, self.ctx.enter(key))



@cython.final
@cython.freelist(200)
cdef class SequenceGenerator:
    cdef object iterable
    cdef object iterator
    cdef SerializerCtx ctx

    def __cinit__(self, iterable, SerializerCtx ctx):
        self.iterable = iterable
        self.ctx = ctx.enter("*")

    def __iter__(self):
        self.iterator = iter(self.iterable)
        return self

    def __next__(self):
        item = next(self.iterator)
        return create_serializable(item, self.ctx)



ItemsView.register(MappingGenerator)
ItemsView.register(EntitySerializer)


cdef class SkipSerialization(EntityAttributeExt):
    pass
