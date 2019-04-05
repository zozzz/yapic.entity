import sys
import cython
from cpython.ref cimport Py_XDECREF, Py_XINCREF, Py_DECREF, Py_INCREF, Py_CLEAR
from cpython.object cimport PyObject, PyObject_RichCompareBool, Py_EQ
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack

from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeImpl
from ._expression cimport Expression, Visitor
from ._field cimport Field, ForeignKey, collect_foreign_keys
from ._factory cimport Factory, ForwardDecl, new_instance_from_forward, is_forward_decl


cdef class Relation(EntityAttribute):
    def __get__(self, instance, owner):
        if instance is None:
            return self
        elif isinstance(instance, EntityBase):
            return (<EntityBase>instance).__rstate__.get_value(self.index)
        else:
            raise TypeError("Instance must be 'None' or 'EntityBase'")

    def __set__(self, EntityBase instance, value):
        instance.__rstate__.set_value(self.index, value)

    def __delete__(self, EntityBase instance):
        instance.__rstate__.del_value(self.index)

    def __getattr__(self, name):
        cdef EntityType joined = self._impl_.joined
        cdef EntityAttribute attr = getattr(joined, name)
        return RelationAttribute(self, attr)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation(self)

    def __repr__(self):
        return "<Relation %s :: %s>" % (self._entity_, self._impl_)

    cdef bind(self, EntityType entity):
        EntityAttribute.bind(self, entity)

    cpdef object clone(self):
        cdef EntityAttribute res = type(self)(self._impl_.clone())
        res._exts_ = self.clone_exts(res)
        return res


cdef class RelationAttribute(Expression):
    def __cinit__(self, Relation relation, EntityAttribute attr):
        self.relation = relation
        self.attr = attr

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation_attribute(self)

    def __getattr__(self, name):
        return getattr(self.attr, name)

    def __repr__(self):
        return "<RelationAttribute %s :: %s>" % (self.relation, self.attr.name)


cdef class RelationImpl(EntityAttributeImpl):
    def __cinit__(self, joined, value_t, *args):
        self.joined = joined
        self.set_value_store_type(value_t)

    cpdef init(self, EntityType entity):
        self.determine_join_expr(entity)

    cdef object new_value_store(self):
        return self.value_store_factory.invoke()

    cdef void set_value_store_type(self, object t):
        if self.value_store_t != t:
            self.value_store_t = t
            self.value_store_factory = Factory.create(t)

    cdef determine_join_expr(self, EntityType entity):
        raise NotImplementedError()

    cpdef object clone(self):
        return type(self)(self.joined, self.value_store_t)


cdef class ManyToOne(RelationImpl):
    def __repr__(self):
        return "ManyToOne %r" % self.joined

    cdef determine_join_expr(self, EntityType entity):
        self.join_expr = determine_join_expr(entity, self.joined)


cdef class OneToMany(RelationImpl):
    def __repr__(self):
        return "OneToMany %r" % self.joined

    cdef determine_join_expr(self, EntityType entity):
        self.join_expr = determine_join_expr(entity, self.joined)


cdef class ManyToMany(RelationImpl):
    def __cinit__(self, joined, value_t, across):
        self.across = across

    def __repr__(self):
        return "ManyToMany %r => %r" % (self.across, self.joined)

    cdef determine_join_expr(self, EntityType entity):
        self.across_join_expr = determine_join_expr(self.across, entity)
        self.join_expr = determine_join_expr(self.across, self.joined)

    cpdef object clone(self):
        return type(self)(self.joined, self.value_store_t, self.across)


cdef determine_join_expr(EntityType entity, EntityType joined):
    cdef dict fks = collect_foreign_keys(entity)

    cdef list keys
    cdef Field field
    cdef ForeignKey fk
    cdef object found = None

    for fk_name, keys in fks.items():
        fk = <ForeignKey>keys[0]

        if fk.ref._entity_ is joined:
            if found is not None:
                raise RuntimeError("Multiple join conditions between %s <-> %s" % (entity, joined))

            found = fk.attr == fk.ref
            for i in range(1, len(keys)):
                fk = <ForeignKey>keys[i]
                found &= fk.attr == fk.ref

    if found is None:
        raise RuntimeError("Can't determine join condition between %s <-> %s" % (entity, joined))

    return found


# ****************************************************************************
# ** VALUE STORES **
# ****************************************************************************

@cython.final
@cython.freelist(1000)
cdef class RelationState:
    def __cinit__(self, tuple relations):
        cdef int size = PyTuple_GET_SIZE(relations)
        self.data = PyTuple_New(size)
        cdef PyObject* data = <PyObject*>self.data

        cdef Relation rel
        cdef RelationImpl impl
        cdef object store

        for i from 0 <= i < size:
            rel = <Relation>PyTuple_GET_ITEM(relations, i)
            impl = <RelationImpl>rel._impl_
            store = impl.new_value_store()
            Py_INCREF(<object>store)
            PyTuple_SET_ITEM(<object>data, i, <object>store)

    cdef object get_value(self, int index):
        cdef ValueStore vs = <ValueStore>self.data[index]
        return vs.get_value()

    cdef bint set_value(self, int index, object value):
        cdef ValueStore vs = <ValueStore>self.data[index]
        return vs.set_value(value)

    cdef bint del_value(self, int index):
        cdef ValueStore vs = <ValueStore>self.data[index]
        return vs.del_value()

    cpdef reset(self):
        cdef int size = PyTuple_GET_SIZE(self.data)

        cdef ValueStore store

        for i from 0 <= i < size:
            store = <ValueStore>self.data[i]
            store.reset()


cdef class ValueStore:
    cdef object get_value(self):
        raise NotImplementedError()

    cdef bint set_value(self, object value):
        raise NotImplementedError()

    cdef bint del_value(self):
        raise NotImplementedError()

    cpdef reset(self):
        raise NotImplementedError()


cdef class RelatedItem(ValueStore):
    def __cinit__(self):
        self.original = NULL
        self.current = NULL

    cdef object get_value(self):
        if self.current is NULL:
            if self.original is NULL:
                return None
            else:
                return <object>self.original
        else:
            return <object>self.current

    cdef bint set_value(self, object value):
        if self.current is NULL:
            self.current = <PyObject*>value
            Py_XINCREF(self.current)
        elif self.original is NULL or not PyObject_RichCompareBool(<object>self.original, value, Py_EQ):
            Py_XDECREF(self.current)
            self.current = <PyObject*>value
            Py_XINCREF(self.current)
        return 1

    cdef bint del_value(self):
        Py_XDECREF(self.current)
        self.current = <PyObject*>None
        Py_XINCREF(self.current)
        return 1

    cpdef reset(self):
        if self.current is not NULL:
            Py_XDECREF(self.original)
            self.original = self.current
            self.current = NULL

    def __dealloc__(self):
        Py_CLEAR(self.original)
        Py_CLEAR(self.current)


cdef class RelatedContainer(ValueStore):
    def __cinit__(self):
        # todo: maybe use set instead of list
        self.__removed__ = []
        self.__added__ = []

    cpdef _op_add(self, object value):
        try:
            key = self.__removed__.index(value)
        except:
            pass
        else:
            del self.__removed__[key]

        if value not in self.__added__:
            self.__added__.append(value)

    cpdef _op_del(self, object value):
        try:
            key = self.__added__.index(value)
        except:
            pass
        else:
            del self.__added__[key]

        if value not in self.__removed__:
            self.__removed__.append(value)

    cpdef reset(self):
        self.__added__ = []
        self.__removed__ = []


cdef class RelatedList(RelatedContainer):
    def __cinit__(self):
        self.value = []

    cdef object get_value(self):
        return self

    cdef bint set_value(self, object value):
        for item in self.value:
            self._op_del(item)

        if isinstance(value, tuple):
            self.value = list(<tuple>value)
        else:
            self.value = value

        for item in self.value:
            self._op_add(item)
        return 1

    cdef bint del_value(self):
        for item in self.value:
            self._op_del(item)
        self.value = []
        return 1

    cpdef append(self, object o):
        self.value.append(o)
        self._op_add(o)

    cpdef extend(self, list o):
        self.value.extend(o)
        for item in o:
            self._op_add(item)

    cpdef insert(self, object index, object o):
        self.value.insert(index, o)
        self._op_add(o)

    cpdef remove(self, object o):
        self.value.remove(o)
        self._op_del(o)

    cpdef pop(self, object index = None):
        item = self.value.pop(index)
        self._op_del(item)
        return item

    cpdef clear(self):
        for item in self.value:
            self._op_del(item)
        del self.value[:]

    cpdef reset(self):
        del self.__removed__[:]
        del self.__added__[:]

    def __getitem__(self, key):
        return self.value[key]

    def __setitem__(self, key, value):
        self._op_delitems_by_key(key)

        self.value[key] = value

        if isinstance(key, int):
            self._op_add(value)
        elif isinstance(key, slice):
            for x in value:
                self._op_add(x)

    def __delitem__(self, key):
        self._op_delitems_by_key(key)
        del self.value[key]

    def __len__(self):
        return len(self.value)

    def __contains__(self, value):
        return value in self.value

    def __iter__(self):
        return iter(self.value)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return repr(self.value)

    cdef void _op_delitems_by_key(self, key):
        if isinstance(key, int):
            if key >= 0 and key < len(self.value):
                self._op_del(self.value[key])
        elif isinstance(key, slice):
            for x in xrange(*key.indices(len(self.value))):
                self._op_del(self.value[x])


cdef class RelatedDict(RelatedContainer):
    cpdef get(self, object key, object dv = None):
        raise NotImplementedError()

    cpdef items(self):
        raise NotImplementedError()

    cpdef keys(self):
        raise NotImplementedError()

    cpdef pop(self, object key, object dv = None):
        raise NotImplementedError()

    cpdef popitem(self):
        raise NotImplementedError()

    cpdef setdefault(self, object key, object dv = None):
        raise NotImplementedError()

    cpdef update(self, object other):
        raise NotImplementedError()

    cpdef values(self):
        raise NotImplementedError()

    cpdef clear(self):
        raise NotImplementedError()

    cpdef reset(self):
        raise NotImplementedError()

    # __getitem__
    # __setitem__
    # __delitem__
    # __len__
    # __contains__
    # __iter__
