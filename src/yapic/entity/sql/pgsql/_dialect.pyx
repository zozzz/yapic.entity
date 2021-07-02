from yapic.entity._entity cimport EntityType
from yapic.entity._expression cimport RawExpression
from yapic.entity._field cimport StorageTypeFactory

from .._dialect cimport Dialect
from .._ddl cimport DDLCompiler, DDLReflect
from .._query cimport QueryCompiler

from ._ddl cimport PostgreDDLCompiler, PostgreDDLReflect
from ._query_compiler cimport PostgreQueryCompiler
from ._type_factory cimport PostgreTypeFactory

cdef class PostgreDialect(Dialect):
    cpdef DDLCompiler create_ddl_compiler(self):
        return PostgreDDLCompiler(self)

    cpdef DDLReflect create_ddl_reflect(self, EntityType base):
        return PostgreDDLReflect(self, base)

    cpdef QueryCompiler create_query_compiler(self):
        return PostgreQueryCompiler(self)

    cpdef StorageTypeFactory create_type_factory(self):
        return PostgreTypeFactory(self)

    cpdef str quote_ident(self, str ident):
        ident = ident.replace('"', '""')
        return f'"{ident}"'

    cpdef list unquote_ident(self, str ident):
        cdef list result = []

        while ident:
            if ident[0] == '"':
                end = chr_pos(ident, '"', 1)
                if end == -1:
                    raise ValueError("Unterminated ident escape")

                result.append(ident[1:end])

                dot = chr_pos(ident, '.')
                if dot == -1:
                    break
                else:
                    ident = ident[dot+1:]
            else:
                dot = chr_pos(ident, '.')
                if dot == -1:
                    result.append(ident.lower())
                    break
                else:
                    result.append(ident[0:dot].lower())
                    ident = ident[dot+1:]

        return result


    cpdef object quote_value(self, object value):
        if isinstance(value, RawExpression):
            return (<RawExpression>value).expr
        elif isinstance(value, int) or isinstance(value, float):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif value is None:
            return "NULL"
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


cdef int chr_pos(str inp, str char, int begin=0):
    cdef int length = len(inp)

    while True:
        if begin >= length:
            break

        c = inp[begin]
        if c == "\\":
            begin += 2
            continue
        elif c == char:
            return begin
        else:
            begin += 1

    return -1
