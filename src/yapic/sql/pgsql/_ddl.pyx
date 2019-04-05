# from hashids import Hashids

from yapic.entity._field cimport Field, PrimaryKey, ForeignKey
from yapic.entity._field_impl cimport IntImpl, StringImpl, ChoiceImpl

from .._ddl cimport DDLCompiler


cdef class PostgreDLLCompiler(DDLCompiler):
    def gues_type(self, Field field):
        impl = field._impl_

        if isinstance(impl, IntImpl):
            return self._int_type(field, <IntImpl>impl)
        elif isinstance(impl, StringImpl):
            return self._string_type(field, <StringImpl>impl)
        elif isinstance(impl, ChoiceImpl):
            return self._choice_type(field, <ChoiceImpl>impl)

    cdef _int_type(self, Field field, IntImpl impl):
        pk = field.get_ext(PrimaryKey)
        if pk is not None:
            if (<PrimaryKey>pk).auto_increment:
                return PostgreType("SERIAL" if field.max_size <= 0 else f"SERIAL{field.max_size}")

        return PostgreType("INT" if field.max_size <= 0 else f"INT{field.max_size}")


    cdef _string_type(self, Field field, StringImpl impl):
        if field.min_size >= 0 and field.max_size > 0:
            if field.min_size == field.max_size:
                return PostgreType("CHAR(%s)" % field.max_size)
            elif field.max_size <= 4000:
                return PostgreType("VARCHAR(%s)" % field.max_size)
        return PostgreType("TEXT")



    cdef _choice_type(self, Field field, ChoiceImpl impl):
        # hashid = Hashids(min_length=5, salt=impl.enum.__qualname__)
        # uid = f"{impl.enum.__name__}_{hashid.encode(1)}"

        # return PostgreType(uid, f"/* ENUM {uid} SQL WORK IN PROGRESS */")

        type = int
        str_max_len = 0
        int_max_size = 0

        for entry in impl._enum:
            value = entry.value
            if isinstance(value, int):
                int_max_size = max(int_max_size, value)
            elif isinstance(value, str) :
                type = str
                str_max_len = max(str_max_len, len(value))

        if impl.is_multi:
            if type is not int:
                raise TypeError("Choice of Flags must be only contains int values")

            return PostgreType(f"BIT({len(impl._enum)})")
        else:
            if type is int:
                if int_max_size < 32767:
                    return PostgreType("INT2")
                elif int_max_size < 2147483647:
                    return PostgreType("INT4")
                else:
                    return PostgreType("INT8")
            elif type is str:
                values = [self.dialect.quote_value(entry.value) for entry in impl._enum]

                return PostgreType(f"VARCHAR({str_max_len}) CHECK(\"{field._name_}\" IN ({', '.join(values)}))")


cdef class PostgreType(StorageType):
    def __cinit__(self, str name, str sql = None):
        self.name = name
        self.pre_sql = sql

    cpdef requirements(self):
        return self.pre_sql
