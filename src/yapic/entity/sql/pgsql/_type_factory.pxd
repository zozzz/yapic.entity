from yapic.entity._field cimport Field, StorageType, StorageTypeFactory
from yapic.entity._field_impl cimport StringImpl, BytesImpl, IntImpl, BoolImpl, DateImpl, DateTimeImpl, DateTimeTzImpl, ChoiceImpl


cdef class PostgreTypeFactory(StorageTypeFactory):
    # XXX: maybe move into parent, and generalize
    cdef object quote_value(self, object value)
    cdef StorageType __int_type(self, Field field, IntImpl impl)
    cdef StorageType __string_type(self, Field field, StringImpl impl)
    cdef StorageType __bytes_type(self, Field field, BytesImpl impl)
    cdef StorageType __bool_type(self, Field field, BoolImpl impl)
    cdef StorageType __date_type(self, Field field, DateImpl impl)
    cdef StorageType __date_time_type(self, Field field, DateTimeImpl impl)
    cdef StorageType __date_time_tz_type(self, Field field, DateTimeTzImpl impl)
    cdef StorageType __choice_type(self, Field field, ChoiceImpl impl)


cdef class PostgreType(StorageType):
    cdef readonly str pre_sql
    cdef readonly str post_sql
