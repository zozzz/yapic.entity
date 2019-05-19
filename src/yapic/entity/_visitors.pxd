from ._expression cimport Expression, Visitor
from ._entity cimport EntityType, EntityBase


cdef class ReplacerBase(Visitor):
    pass


cdef class Walk(Visitor):
    pass


cdef class EntityReplacer(ReplacerBase):
    cdef EntityType what
    cdef EntityType to


cdef inline replace_entity(Expression expr, EntityType what, EntityType to):
    return EntityReplacer(what, to).visit(expr)


cdef class FieldAssigner(ReplacerBase):
    cdef EntityType where_t
    cdef EntityBase where_o
    cdef dict data


cdef inline assign_field(EntityBase into, Expression expr, list values):
    cdef dict data = {}
    for r in values:
        data[type(r)] = r
    FieldAssigner(type(into), into, data).visit(expr)


cdef class FieldExtractor(Walk):
    cdef EntityType entity
    cdef list fields


cdef inline tuple extract_fields(EntityType entity, Expression expr):
    fe = FieldExtractor(entity)
    fe.visit(expr)
    return tuple(fe.fields)


cdef class FieldReplacer(ReplacerBase):
    cdef tuple fields
    cdef tuple values


cdef inline Expression replace_fields(Expression expr, tuple fields, tuple values):
    return FieldReplacer(fields, values).visit(expr)
