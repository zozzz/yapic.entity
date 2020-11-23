from yapic.entity._field cimport Field, StorageType, StorageTypeFactory
from yapic.entity._field_impl cimport (
    StringImpl,
    BytesImpl,
    IntImpl,
    BoolImpl,
    DateImpl,
    DateTimeImpl,
    DateTimeTzImpl,
    TimeImpl,
    TimeTzImpl,
    NumericImpl,
    FloatImpl,
    UUIDImpl,
    ChoiceImpl,
    JsonImpl,
    CompositeImpl,
    ArrayImpl
)
from yapic.entity._geom_impl cimport (
    PointImpl,
)
from .postgis._impl cimport (
    PostGISPointImpl,
    PostGISLatLngImpl,
)


from ._dialect cimport PostgreDialect


cdef class PostgreTypeFactory(StorageTypeFactory):
    cdef PostgreDialect dialect

    cpdef StorageType _create(self, Field field, object impl)
    cdef StorageType __int_type(self, Field field, IntImpl impl)
    cdef StorageType __string_type(self, Field field, StringImpl impl)
    cdef StorageType __bytes_type(self, Field field, BytesImpl impl)
    cdef StorageType __bool_type(self, Field field, BoolImpl impl)
    cdef StorageType __date_type(self, Field field, DateImpl impl)
    cdef StorageType __date_time_type(self, Field field, DateTimeImpl impl)
    cdef StorageType __date_time_tz_type(self, Field field, DateTimeTzImpl impl)
    cdef StorageType __time_type(self, Field field, TimeImpl impl)
    cdef StorageType __time_tz_type(self, Field field, TimeTzImpl impl)
    cdef StorageType __numeric_type(self, Field field, NumericImpl impl)
    cdef StorageType __float_type(self, Field field, FloatImpl impl)
    cdef StorageType __uuid_type(self, Field field, UUIDImpl impl)
    cdef StorageType __choice_type(self, Field field, ChoiceImpl impl)
    cdef StorageType __json_type(self, Field field, JsonImpl impl)
    cdef StorageType __composite_type(self, Field field, CompositeImpl impl)
    cdef StorageType __array_type(self, Field field, ArrayImpl impl)
    cdef StorageType __point_type(self, Field field, PointImpl impl)
    cdef StorageType __postgis_point_type(self, Field field, PostGISPointImpl impl)
    cdef StorageType __postgis_longlat_type(self, Field field, PostGISLatLngImpl impl)


cdef class PostgreType(StorageType):
    cdef readonly str pre_sql
    cdef readonly str post_sql
