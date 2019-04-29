from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field, StorageType

from ._ddl cimport DDLCompiler, DDLReflect
from ._query_compiler cimport QueryCompiler


cdef class Dialect:
    cpdef DDLCompiler create_ddl_compiler(self):
        raise NotImplementedError()

    cpdef DDLReflect create_ddl_reflect(self, EntityType base):
        raise NotImplementedError()

    cpdef QueryCompiler create_query_compiler(self):
        raise NotImplementedError()

    cpdef str quote_ident(self, str ident):
        raise NotImplementedError()

    cpdef object quote_value(self, object value):
        raise NotImplementedError()

    cpdef str table_qname(self, EntityType entity):
        raise NotImplementedError()

    cpdef StorageType get_field_type(self, Field field):
        return field.get_type(self.type_factory)

    cpdef bint expression_eq(self, Expression a, Expression b):
        qc = self.create_query_compiler()
        return qc.visit(a) == qc.visit(b)

    cpdef EntityDiff entity_diff(self, EntityType a, EntityType b):
        return EntityDiff(a, b, self.expression_eq)

    cpdef str compile_insert(self, EntityType entity, dict data):
        field_names = [self.quote_ident(k) for k in data.keys()]
        values = [self.quote_value(v) for v in data.values()]
        return f"INSERT INTO {self.table_qname(entity)} ({', '.join(field_names)}) VALUES ({', '.join(values)});"

    cpdef str compile_update(self, EntityType entity, dict data):
        pk_names = [attr._name_ for attr in entity.__pk__]
        update = [f"{self.quote_ident(k)}={self.quote_value(v)}" for k, v in data.items() if k not in pk_names]
        where = [f"{self.quote_ident(k)}={self.quote_value(v)}" for k, v in data.items() if k in pk_names]
        return f"UPDATE {self.table_qname(entity)} SET {', '.join(update)} WHERE {' AND '.join(where)};"

    cpdef str compile_delete(self, EntityType entity, dict data):
        where = [f"{self.quote_ident(k)}={self.quote_value(v)}" for k, v in data.items()]
        return f"DELETE FROM {self.table_qname(entity)} WHERE {' AND '.join(where)};"
