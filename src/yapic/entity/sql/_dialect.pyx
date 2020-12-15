from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field, StorageType

from ._ddl cimport DDLCompiler, DDLReflect
from ._query cimport QueryCompiler


cdef class Dialect:
    cpdef DDLCompiler create_ddl_compiler(self):
        raise NotImplementedError()

    cpdef DDLReflect create_ddl_reflect(self, EntityType base):
        raise NotImplementedError()

    cpdef QueryCompiler create_query_compiler(self):
        raise NotImplementedError()

    cpdef StorageTypeFactory create_type_factory(self):
        raise NotImplementedError()

    cpdef str quote_ident(self, str ident):
        raise NotImplementedError()

    cpdef object quote_value(self, object value):
        raise NotImplementedError()

    cpdef str table_qname(self, EntityType entity):
        raise NotImplementedError()

    cpdef StorageType get_field_type(self, Field field):
        # XXX: optimalize...
        return field.get_type(self.create_type_factory())

    # cpdef object encode_value(self, Field field, object value):
    #     if value is None:
    #         return "NULL"
    #     cdef StorageType t = self.get_field_type(field)
    #     return t.encode(value)

    cpdef bint expression_eq(self, Expression a, Expression b):
        qc = self.create_query_compiler()
        return qc.visit(a) == qc.visit(b)

    cpdef EntityDiff entity_diff(self, EntityType a, EntityType b):
        return EntityDiff(a, b, self.expression_eq)
