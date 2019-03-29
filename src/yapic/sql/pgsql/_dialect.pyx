from yapic.entity._entity cimport EntityType
from .._dialect cimport Dialect
from .._ddl cimport DDLCompiler
from ._ddl cimport PostgreDLLCompiler

cdef class PostgreDialect(Dialect):
    cpdef DDLCompiler create_ddl_compiler(self):
        return PostgreDLLCompiler(self)

    cpdef str quote_ident(self, str ident):
        ident = ident.replace('"', '""')
        return f'"{ident}"'

    cpdef str quote_value(self, object value):
        value = str(value).replace('"', '""')
        return f"'{value}'"

    cpdef str table_qname(self, EntityType entity):
        if "schema" in entity.__meta__:
            return f"{self.quote_ident(entity.__meta__['schema'])}.{self.quote_ident(entity.__name__)}"
        else:
            return self.quote_ident(entity.__name__)
