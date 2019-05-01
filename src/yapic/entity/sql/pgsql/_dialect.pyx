from yapic.entity._entity cimport EntityType
from yapic.entity._expression cimport RawExpression
from yapic.entity._field cimport StorageTypeFactory

from .._dialect cimport Dialect
from .._ddl cimport DDLCompiler, DDLReflect
from .._query_compiler cimport QueryCompiler

from ._ddl cimport PostgreDDLCompiler, PostgreDDLReflect
from ._query_compiler cimport PostgreQueryCompiler
from ._type_factory cimport PostgreTypeFactory

cdef class PostgreDialect(Dialect):
    cpdef DDLCompiler create_ddl_compiler(self):
        return PostgreDDLCompiler(self)

    cpdef DDLReflect create_ddl_reflect(self, EntityType base):
        return PostgreDDLReflect(base)

    cpdef QueryCompiler create_query_compiler(self):
        return PostgreQueryCompiler(self)

    cpdef StorageTypeFactory create_type_factory(self):
        return PostgreTypeFactory(self)

    cpdef str quote_ident(self, str ident):
        ident = ident.replace('"', '""')
        return f'"{ident}"'

    cpdef object quote_value(self, object value):
        if isinstance(value, RawExpression):
            return (<RawExpression>value).expr
        elif isinstance(value, int) or isinstance(value, float):
            return value
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        else:
            value = str(value).replace("'", "''")
            return f"'{value}'"

    cpdef str table_qname(self, EntityType entity):
        try:
            schema = entity.__meta__["schema"]
        except KeyError:
            schema = None

        if schema is not None:
            return f"{self.quote_ident(schema)}.{self.quote_ident(entity.__name__)}"
        else:
            return self.quote_ident(entity.__name__)
