import cython

from ._entity cimport EntityType, EntityBase
from ._expression cimport Expression, Visitor
from ._field cimport Field
from ._factory cimport ForwardDecl, new_instance_from_forward, is_forward_decl


cdef class Relation(Expression):
    def __cinit__(self, impl):
        self._impl = impl

    @property
    def __impl__(self):
        if is_forward_decl(self._impl):
            self._impl = new_instance_from_forward(self._impl)
        return self._impl

    def __get__(self, instance, owner):
        if instance is None:
            return self
        elif isinstance(instance, EntityBase):
            return self.__impl__.get_value()
        else:
            raise TypeError("Instance must be 'None' or 'EntityBase'")

    def __set__(self, EntityBase instance, value):
        self.__impl__.set_value(value)

    def __delete__(self, EntityBase instance):
        self.__impl__.del_value()

    def __getattr__(self, name):
        cdef EntityType joined = self.__impl__.joined
        cdef Field field = getattr(joined, name)
        return RelationField(self, field)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation(self)

    def __repr__(self):
        return "<Relation %s :: %s>" % (self.__entity__, self.__impl__)

    cdef void bind(self, EntityType entity):
        self.__entity__ = entity


cdef class RelationField(Expression):
    def __cinit__(self, Relation relation, Field field):
        self.relation = relation
        self.field = field

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation_field(self)

    def __repr__(self):
        return "<RelationField %s :: %s>" % (self.relation, self.field.name)


cdef class RelationImpl:
    cpdef object get_value(self):
        raise NotImplementedError()

    cpdef void set_value(self, value):
        raise NotImplementedError()

    cpdef void del_value(self):
        raise NotImplementedError()


cdef class ManyToOne(RelationImpl):
    def __cinit__(self, joined, value):
        self.joined = joined
        self.value = value

    cpdef object get_value(self):
        return self.value.get_value()

    cpdef void set_value(self, value):
        self.value.set_value(value)

    cpdef void del_value(self):
        self.value.del_value()

    def __repr__(self):
        return "ManyToOne %r" % self.joined


cdef class OneToMany(RelationImpl):
    def __cinit__(self, joined, value):
        self.joined = joined
        self.value = value

    def __repr__(self):
        return "OneToMany %r" % self.joined


cdef class ManyToMany(RelationImpl):
    def __cinit__(self, joined, across, value):
        self.joined = joined
        self.across = across
        self.value = value

    def __repr__(self):
        return "ManyToMany %r => %r" % (self.across, self.joined)


# ****************************************************************************
# ** VALUE STORES **
# ****************************************************************************

cdef class RelatedItem:
    cdef object get_value(self):
        return self.current

    cdef void set_value(self, object value):
        if self.original is None:
            self.original = value
            self.current = value
        else:
            self.current = value

    cdef void del_value(self):
        self.current = None

    cpdef reset(self):
        self.original = self.current


cdef class RelatedContainer:
    cpdef _set_item(self, object key, object value):
        raise NotImplementedError()

    cpdef _get_item(self, object key):
        raise NotImplementedError()

    cpdef _del_item(self, object key):
        raise NotImplementedError()


cdef class RelatedList(RelatedContainer):
    cpdef append(self, object o):
        raise NotImplementedError()

    cpdef extend(self, object o):
        raise NotImplementedError()

    cpdef insert(self, object index, object o):
        raise NotImplementedError()

    cpdef remove(self, object o):
        raise NotImplementedError()

    cpdef pop(self, object index = None):
        raise NotImplementedError()

    cpdef clear(self):
        raise NotImplementedError()

    cpdef reset(self):
        raise NotImplementedError()

    # def __getitem__(self, object key)

    # __getitem__
    # __setitem__
    # __delitem__
    # __len__
    # __contains__
    # __iter__


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
