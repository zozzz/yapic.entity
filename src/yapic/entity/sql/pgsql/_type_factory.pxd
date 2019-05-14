from yapic.entity._field cimport Field, StorageType, StorageTypeFactory
from yapic.entity._field_impl cimport (
    StringImpl,
    BytesImpl,
    IntImpl,
    BoolImpl,
    DateImpl,
    DateTimeImpl,
    DateTimeTzImpl,
    NumericImpl,
    FloatImpl,
    ChoiceImpl,
    JsonImpl,
    CompositeImpl
)


from ._dialect cimport PostgreDialect


cdef class PostgreTypeFactory(StorageTypeFactory):
    cdef PostgreDialect dialect

    cdef StorageType __int_type(self, Field field, IntImpl impl)
    cdef StorageType __string_type(self, Field field, StringImpl impl)
    cdef StorageType __bytes_type(self, Field field, BytesImpl impl)
    cdef StorageType __bool_type(self, Field field, BoolImpl impl)
    cdef StorageType __date_type(self, Field field, DateImpl impl)
    cdef StorageType __date_time_type(self, Field field, DateTimeImpl impl)
    cdef StorageType __date_time_tz_type(self, Field field, DateTimeTzImpl impl)
    cdef StorageType __numeric_type(self, Field field, NumericImpl impl)
    cdef StorageType __float_type(self, Field field, FloatImpl impl)
    cdef StorageType __choice_type(self, Field field, ChoiceImpl impl)
    cdef StorageType __json_type(self, Field field, JsonImpl impl)
    cdef StorageType __composite_type(self, Field field, CompositeImpl impl)


cdef class PostgreType(StorageType):
    cdef readonly str pre_sql
    cdef readonly str post_sql
