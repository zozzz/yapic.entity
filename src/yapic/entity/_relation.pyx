import cython

from ._entity cimport EntityType
from ._expression cimport Expression, Visitor
from ._field cimport Field


@cython.auto_pickle(False)
cdef class Relation(Expression):
    def __getattr__(self, name):
        cdef EntityType related = self.related
        cdef Field field = getattr(related, name)
        return RelationField(self, field)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation(self)

    def __repr__(self):
        if self.__across__:
            return "<Relation (%s) %s -> %s -> %s>" % (
                self.__impl__.__name__,
                self.__entity__.__name__,
                self.__across__.__name__,
                self.__related__.__name__)
        else:
            return "<Relation (%s) %s -> %s>" % (
                self.__impl__.__name__,
                self.__entity__.__name__,
                self.__related__.__name__)


@cython.auto_pickle(False)
cdef class RelationField(Expression):
    def __cinit__(self, Relation relation, Field field):
        self.relation = relation
        self.field = field

    cpdef visit(self, Visitor visitor):
        return visitor.visit_relation_field(self)

    def __repr__(self):
        if self.relation.__across__:
            return "<RelationField %s -> %s -> %s [%s]>" % (
                self.relation.__entity__.__name__,
                self.relation.__across__.__name__,
                self.relation.__related__.__name__,
                self.field.name)
        else:
            return "<RelationField %s -> %s [%s]>" % (
                self.relation.__entity__.__name__,
                self.relation.__related__.__name__,
                self.field.name)


@cython.auto_pickle(False)
cdef class RelationImpl:
    pass


@cython.auto_pickle(False)
cdef class ManyToOne(RelationImpl):
    pass


@cython.auto_pickle(False)
cdef class OneToMany(RelationImpl):
    pass


@cython.auto_pickle(False)
cdef class ManyToMany(RelationImpl):
    pass
