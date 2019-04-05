from ._expression cimport Expression, Visitor
from ._entity cimport EntityType


cdef class EntityReplacer(Visitor):
    cdef EntityType what
    cdef EntityType to


cdef inline replace_entity(Expression expr, EntityType what, EntityType to):
    return EntityReplacer(what, to).visit(expr)
