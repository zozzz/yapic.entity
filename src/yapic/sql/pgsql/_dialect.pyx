from yapic.entity._entity cimport EntityType
from yapic.entity._expression cimport RawExpression

from .._dialect cimport Dialect
from .._ddl cimport DDLCompiler, DDLReflect
from .._query_compiler cimport QueryCompiler

from ._ddl cimport PostgreDDLCompiler, PostgreDDLReflect
from ._query_compiler cimport PostgreQueryCompiler
from ._type_factory cimport PostgreTypeFactory

cdef class PostgreDialect(Dialect):
    def __cinit__(self):
        self.type_factory = PostgreTypeFactory()

    cpdef DDLCompiler create_ddl_compiler(self):
        return PostgreDDLCompiler(self)

    cpdef DDLReflect create_ddl_reflect(self, EntityType base):
        return PostgreDDLReflect(base)

    cpdef QueryCompiler create_query_compiler(self):
        return PostgreQueryCompiler(self)

    cpdef str quote_ident(self, str ident):
        ident = ident.replace('"', '""')
        return f'"{ident}"'

    cpdef object quote_value(self, object value):
        return (<PostgreTypeFactory>self.type_factory).quote_value(value)

    cpdef str table_qname(self, EntityType entity):
        try:
            schema = entity.__meta__["schema"]
        except KeyError:
            schema = None

        if schema is not None:
            return f"{self.quote_ident(schema)}.{self.quote_ident(entity.__name__)}"
        else:
            return self.quote_ident(entity.__name__)
