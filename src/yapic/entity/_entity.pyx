import cython

from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack

from ._field cimport Field, FieldExtension


@cython.auto_pickle(False)
cdef class EntityType(type):
    def __cinit__(self, name, bases, attrs):
        cdef list fields = []
        cdef list names = []
        cdef FieldExtension ext

        if "__annotations__" in attrs:
            for k, v in attrs["__annotations__"].items():
                if hasattr(v, "__origin__") and issubclass(v.__origin__, Field):
                    names.append(k)

            for name in names:
                ext = None
                try:
                    definition = attrs[name]
                except KeyError:
                    definition = Field()
                else:
                    if isinstance(definition, FieldExtension):
                        ext = definition
                        definition = ext.field

                    if not isinstance(definition, Field):
                        definition = Field(default=definition)

                if ext and not ext.field:
                    ext.field = definition
                    ext.field.extensions.append(ext)

                fields.append(definition)
                setattr(self, name, definition)

        self.__fields__ = tuple(fields)
        self.__field_names__ = tuple(names)

        cdef Field field
        for i, field in enumerate(fields):
            field.index = i
            field.bind(self, names[i])

    @staticmethod
    def __prepare__(name, bases, **kw):
        scope = type.__prepare__(name, bases, **kw)
        scope["__slots__"] = []
        return scope

    def __repr__(self):
        return "<Entity %s>" % self.__name__


@cython.auto_pickle(False)
@cython.final
cdef class EntityState:
    def __cinit__(self, tuple fields, tuple data):
        self.fields = fields
        self.data = data
        self.dirty = PyTuple_New(PyTuple_GET_SIZE(data))

    cdef bint set_value(self, int index, object value):
        cdef PyObject* fields = <PyObject*>self.fields
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef PyObject* newValue = <PyObject*>value
        cdef Field field

        cv = PyTuple_GET_ITEM(<object>data, index)
        if cv == NULL:
            Py_INCREF(<object>newValue)
            PyTuple_SET_ITEM(<object>dirty, index, <object>newValue)
        else:
            field = <Field>PyTuple_GET_ITEM(<object>fields, index)
            if not field.values_is_eq(<object>cv, <object>newValue):
                cv = PyTuple_GET_ITEM(<object>dirty, index)
                if cv != NULL:
                    Py_DECREF(<object>cv)
                Py_INCREF(<object>newValue)
                PyTuple_SET_ITEM(<object>dirty, index, <object>newValue)

    cdef object get_value(self, int index):
        cdef PyObject* container = <PyObject*>self.dirty
        cdef PyObject* cv = PyTuple_GET_ITEM(<object>container, index)

        if cv == NULL:
            container = <PyObject*>self.data
            cv = PyTuple_GET_ITEM(<object>container, index)
            if cv == NULL:
                return None

        Py_INCREF(<object>cv)
        return <object>cv

    cdef void del_value(self, int index):
        cdef PyObject* container = <PyObject*>self.dirty
        cdef PyObject* cv = PyTuple_GET_ITEM(<object>container, index)
        if cv != NULL:
            Py_DECREF(<object>cv)
        PyTuple_SET_ITEM(<object>container, index, <object>NULL)

    @property
    def changes(self):
        cdef PyObject* fields = <PyObject*>self.fields
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef PyObject* ov
        cdef PyObject* field

        for i from 0 <= i < PyTuple_GET_SIZE(<object>dirty):
            cv = PyTuple_GET_ITEM(<object>dirty, i)
            if cv == NULL:
                continue

            ov = PyTuple_GET_ITEM(<object>data, i)
            if ov == NULL:
                ov = <PyObject*>None

            field = PyTuple_GET_ITEM(<object>fields, i)
            yield PyTuple_Pack(3, <PyObject*>field, ov, cv)

    cpdef reset(self):
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef int l = PyTuple_GET_SIZE(<object>dirty)

        for i from 0 <= i < l:
            cv = PyTuple_GET_ITEM(<object>dirty, i)
            if cv == NULL:
                cv = PyTuple_GET_ITEM(<object>data, i)
                if cv == NULL:
                    cv = <PyObject*>None
                Py_INCREF(<object>cv)
                PyTuple_SET_ITEM(<object>dirty, i, <object>cv)

        self.data = <tuple>dirty
        self.dirty = PyTuple_New(l)

    def __iter__(self):
        cdef PyObject* fields = <PyObject*>self.fields
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef PyObject* field

        for i from 0 <= i < PyTuple_GET_SIZE(<object>dirty):
            cv = PyTuple_GET_ITEM(<object>dirty, i)
            if cv == NULL:
                cv = PyTuple_GET_ITEM(<object>data, i)
                if cv == NULL:
                    cv = <PyObject*>None

            field = PyTuple_GET_ITEM(<object>fields, i)
            yield PyTuple_Pack(2, <PyObject*>(<Field>field).name, cv)

    def __repr__(self):
        return "<EntityState %r>" % (dict(self),)


@cython.auto_pickle(False)
cdef class EntityBase:
    def __cinit__(self):
        cdef EntityType model = type(self)
        cdef tuple fields = <tuple>model.__fields__
        cdef tuple data = PyTuple_New(PyTuple_GET_SIZE(fields))
        self.__state__ = EntityState(fields, data)


class Entity(EntityBase, metaclass=EntityType):
    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, "TODO: PK_VALUE")
