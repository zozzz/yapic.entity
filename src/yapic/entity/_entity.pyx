import cython

from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack

from ._field cimport Field, FieldExtension
from ._relation cimport Relation
from ._factory cimport Factory, get_type_hints


cdef class EntityType(type):
    def __cinit__(self, name, bases, attrs):
        cdef list fields = []
        cdef list relations = []
        cdef list names = []

        cdef Field field
        cdef Relation relation

        cdef tuple hints = get_type_hints(self)

        if hints[1] is not None:
            for name, type in (<dict>hints[1]).items():
                factory = Factory.create(type, Field)

                try:
                    value = attrs[name]
                except KeyError:
                    value = None

                if factory is not None:
                    field = init_field(factory(), value)

                    if not field.name:
                        field.name = name

                    fields.append(field)
                    names.append(name)
                    setattr(self, name, field)
                else:
                    factory = Factory.create(type, Relation)
                    if factory is not None:
                        relation = init_relation(factory(), value)
                        relations.append(relation)
                        setattr(self, name, relation)


        self.__fields__ = tuple(fields)
        self.__field_names__ = tuple(names)

        for i, field in enumerate(fields):
            field.index = i
            field.bind(self)

        for relation in relations:
            relation.bind(self)

    @staticmethod
    def __prepare__(name, bases, **kw):
        scope = type.__prepare__(name, bases, **kw)
        scope["__slots__"] = []
        return scope

    def __repr__(self):
        return "<Entity %s>" % self.__name__


cdef Field init_field(Field by_type, object value):
    cdef Field field
    cdef FieldExtension ext

    if isinstance(value, Field):
        field = <Field>value
        field.impl = by_type.impl
        return field
    elif isinstance(value, FieldExtension):
        ext = <FieldExtension>value

        if ext.field:
            by_type.extensions = ext.field.extensions
        else:
            by_type.extensions = [ext]

        for ext in by_type.extensions:
            ext.field = by_type

        return by_type
    else:
        by_type._default = value
        return by_type


cdef Relation init_relation(Relation by_type, object value):
    return by_type


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


cdef class EntityBase:
    def __cinit__(self):
        cdef EntityType model = type(self)
        cdef tuple fields = <tuple>model.__fields__
        cdef tuple data = PyTuple_New(PyTuple_GET_SIZE(fields))
        self.__state__ = EntityState(fields, data)


class Entity(EntityBase, metaclass=EntityType):
    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, "TODO: PK_VALUE")
