from datetime import date, datetime, time
from decimal import Decimal
from cpython.object cimport PyObject
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject

from yapic import json
from yapic.entity._entity cimport EntityType, EntityBase
from yapic.entity._field cimport Field, PrimaryKey, StorageType, StorageTypeFactory
from yapic.entity._expression cimport RawExpression
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
    ChoiceImpl,
    JsonImpl,
    CompositeImpl,
    UUIDImpl,
    ArrayImpl,
    AutoImpl,
)
from yapic.entity._geom_impl cimport (
    PointImpl,
)
from .postgis._impl import DEFAULT_SRID
from .postgis._impl cimport (
    PostGISPointImpl,
    PostGISLatLngImpl,
)

from ._dialect cimport PostgreDialect


cdef class PostgreTypeFactory(StorageTypeFactory):
    def __cinit__(self, PostgreDialect dialect):
        self._dialect_ref = <object>PyWeakref_NewRef(dialect, None)

    cdef PostgreDialect get_dialect(self):
        if self._dialect_ref is not None:
            return <object>PyWeakref_GetObject(<object>self._dialect_ref)
        return None

    cpdef StorageType create(self, Field field):
        return self._create(field, field._impl_)

    cpdef StorageType _create(self, Field field, object impl):
        if isinstance(impl, IntImpl):
            return self.__int_type(field, <IntImpl>impl)
        elif isinstance(impl, StringImpl):
            return self.__string_type(field, <StringImpl>impl)
        elif isinstance(impl, BytesImpl):
            return self.__bytes_type(field, <BytesImpl>impl)
        elif isinstance(impl, BoolImpl):
            return self.__bool_type(field, <BoolImpl>impl)
        elif isinstance(impl, DateImpl):
            return self.__date_type(field, <DateImpl>impl)
        elif isinstance(impl, DateTimeImpl):
            return self.__date_time_type(field, <DateTimeImpl>impl)
        elif isinstance(impl, DateTimeTzImpl):
            return self.__date_time_tz_type(field, <DateTimeTzImpl>impl)
        elif isinstance(impl, TimeImpl):
            return self.__time_type(field, <TimeImpl>impl)
        elif isinstance(impl, TimeTzImpl):
            return self.__time_tz_type(field, <TimeTzImpl>impl)
        elif isinstance(impl, NumericImpl):
            return self.__numeric_type(field, <NumericImpl>impl)
        elif isinstance(impl, FloatImpl):
            return self.__float_type(field, <FloatImpl>impl)
        elif isinstance(impl, UUIDImpl):
            return self.__uuid_type(field, <UUIDImpl>impl)
        elif isinstance(impl, ChoiceImpl):
            return self.__choice_type(field, <ChoiceImpl>impl)
        elif isinstance(impl, JsonImpl):
            return self.__json_type(field, <JsonImpl>impl)
        elif isinstance(impl, PointImpl):
            return self.__point_type(field, <PointImpl>impl)
        elif isinstance(impl, PostGISPointImpl):
            return self.__postgis_point_type(field, <PostGISPointImpl>impl)
        elif isinstance(impl, PostGISLatLngImpl):
            return self.__postgis_longlat_type(field, <PostGISLatLngImpl>impl)
        elif isinstance(impl, CompositeImpl):
            return self.__composite_type(field, <CompositeImpl>impl)
        elif isinstance(impl, ArrayImpl):
            return self.__array_type(field, <ArrayImpl>impl)
        elif isinstance(impl, AutoImpl):
            return self.__auto_type(field, <AutoImpl>impl)

    cdef StorageType __int_type(self, Field field, IntImpl impl):
        # pk = field.get_ext(PrimaryKey)
        # if pk is not None:
        #     if (<PrimaryKey>pk).auto_increment:
        #         return IntType("SERIAL" if field.max_size <= 0 else f"SERIAL{field.max_size}")

        return IntType("INT" if field.max_size <= 0 else f"INT{field.max_size}")

    cdef StorageType __string_type(self, Field field, StringImpl impl):
        if field.min_size >= 0 and field.max_size > 0:
            if field.min_size == field.max_size:
                return StringType("CHAR(%s)" % field.max_size)
            elif field.max_size <= 4000:
                return StringType("VARCHAR(%s)" % field.max_size)
        return StringType("TEXT")

    cdef StorageType __bytes_type(self, Field field, BytesImpl impl):
        return BytesType("BYTEA")

    cdef StorageType __bool_type(self, Field field, BoolImpl impl):
        return BoolType("BOOLEAN")

    cdef StorageType __date_type(self, Field field, DateImpl impl):
        return DateType("DATE")

    cdef StorageType __date_time_type(self, Field field, DateTimeImpl impl):
        return DateTimeType("TIMESTAMP")

    cdef StorageType __date_time_tz_type(self, Field field, DateTimeTzImpl impl):
        return DateTimeTzType("TIMESTAMPTZ")

    cdef StorageType __time_type(self, Field field, TimeImpl impl):
        return TimeType("TIME")

    cdef StorageType __time_tz_type(self, Field field, TimeTzImpl impl):
        return TimeType("TIMETZ")

    cdef StorageType __numeric_type(self, Field field, NumericImpl impl):
        if field.min_size <= 0 or field.max_size <= 0:
            raise ValueError(f"Invalid values for Numeric type: {(field.min_size, field.max_size)}")

        return NumericType(f"NUMERIC({field.min_size}, {field.max_size})")

    cdef StorageType __float_type(self, Field field, FloatImpl impl):
        return FloatType(f"FLOAT{field.max_size}")

    cdef StorageType __uuid_type(self, Field field, UUIDImpl impl):
        return UUIDType(f"UUID")

    cdef StorageType __choice_type(self, Field field, ChoiceImpl impl):
        cdef StorageType value_type = self._create(field, impl._ref_impl)
        cdef ChoiceType t = ChoiceType(value_type.name, value_type.pre_sql, value_type.post_sql)
        t.value_type = value_type
        t.enum = impl._enum
        return t

    cdef StorageType __auto_type(self, Field field, AutoImpl impl):
        return self._create(field, impl._ref_impl)

    cdef StorageType __json_type(self, Field field, JsonImpl impl):
        cdef JsonType t = JsonType("JSONB")
        t._object = impl._object_
        t._list = impl._list_
        t._any = impl._any_
        return t

    cdef StorageType __composite_type(self, Field field, CompositeImpl impl):
        cdef CompositeType t = CompositeType(self.get_dialect().table_qname(impl._entity_))
        t.entity = impl._entity_
        return t

    cdef StorageType __array_type(self, Field field, ArrayImpl impl):
        cdef StorageType item_type = self._create(field, impl._item_impl_)
        cdef ArrayType t = ArrayType(f"{item_type.name}[]")
        t.item_type = item_type
        return t

    cdef StorageType __point_type(self, Field field, PointImpl impl):
        return PointType("POINT")

    cdef StorageType __postgis_point_type(self, Field field, PostGISPointImpl impl):
        return PostGISPointType("POINT", DEFAULT_SRID)

    cdef StorageType __postgis_longlat_type(self, Field field, PostGISLatLngImpl impl):
        return PostGISLatLngType("POINT", DEFAULT_SRID)


cdef class PostgreType(StorageType):
    def __init__(self, str name, str pre_sql = None, str post_sql = None):
        self.name = name
        self.pre_sql = pre_sql
        self.post_sql = post_sql


cdef class IntType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        if isinstance(value, int):
            return value
        else:
            return int(value)

    cpdef object decode(self, object value):
        if value is None:
            return None

        if isinstance(value, int):
            return value
        else:
            return int(value)


cdef class StringType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        if isinstance(value, str):
            return value
        elif isinstance(value, bytes):
            return value.decode("UTF-8")
        else:
            return str(value)

    cpdef object decode(self, object value):
        return value


cdef class BytesType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        if not isinstance(value, bytes):
            raise ValueError("Bytes type only accepts byte strings")
        return value

    cpdef object decode(self, object value):
        return value


cdef class BoolType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        return RawExpression("TRUE" if bool(value) else "FALSE")

    cpdef object decode(self, object value):
        if isinstance(value, bool):
            return value
        elif not isinstance(value, str):
            value = str(value)
        return value.lower() in ("true", "t", "1", "y", "yes", "on")


cdef class DateType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        return RawExpression("'" + value.strftime("%Y-%m-%d") + "'")

    cpdef object decode(self, object value):
        if value is None:
            return None

        if isinstance(value, date):
            return value
        return datetime.strptime(value, "%Y-%m-%d").date()


cdef class DateTimeType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        return RawExpression("'" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'")

    cpdef object decode(self, object value):
        if value is None:
            return None

        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")


cdef class DateTimeTzType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        if value.utcoffset() is None:
            raise ValueError("datetime value must have timezone information")
        return RawExpression("'" + value.strftime("%Y-%m-%d %H:%M:%S.%f%z") + "'")

    cpdef object decode(self, object value):
        if value is None:
            return None

        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f%z")


cdef class TimeType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        return RawExpression("'" + value.isoformat() + "'")

    cpdef object decode(self, object value):
        if value is None:
            return None

        if isinstance(value, time):
            return value
        else:
            return time.fromisoformat(value)


cdef class TimeTzType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        if value.utcoffset() is None:
            raise ValueError("time value must have timezone information")
        return RawExpression("'" + value.isoformat() + "'")

    cpdef object decode(self, object value):
        if value is None:
            return None

        if isinstance(value, time):
            return value
        else:
            return time.fromisoformat(value)


cdef class NumericType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None
        return str(value)

    cpdef object decode(self, object value):
        if value is None:
            return None

        return Decimal(value)


cdef class FloatType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None

        if isinstance(value, str):
            return float(value)
        else:
            return value

    cpdef object decode(self, object value):
        if value is None:
            return None

        if isinstance(value, str):
            return float(value)
        else:
            return value


cdef class UUIDType(PostgreType):
    cpdef object encode(self, object value):
        return value

    cpdef object decode(self, object value):
        return value


cdef class ChoiceType(PostgreType):
    cdef StorageType value_type
    cdef object enum

    cpdef object encode(self, object value):
        if isinstance(value, self.enum):
            return self.value_type.encode(value.value)
        else:
            return self.value_type.encode(value)

    cpdef object decode(self, object value):
        if isinstance(value, self.enum):
            return value
        else:
            value = self.value_type.decode(value)
            for entry in self.enum:
                if entry.value == value:
                    return entry
        return value


cdef class JsonType(PostgreType):
    cdef EntityType _object
    cdef EntityType _list
    cdef bint _any

    cpdef object encode(self, object value):
        if value is None:
            return None
        elif self._object:
            if isinstance(value, EntityBase):
                if type(value) is not self._object:
                    raise ValueError("Missmatch entity type: %r expected %r" % (type(value), self._object))
                return json.dumps(value.as_dict(), ensure_ascii=False)
        elif self._list:
            if isinstance(value, list):
                return json.dumps([v.as_dict() for v in value], ensure_ascii=False)
        elif self._any:
            return json.dumps(value, ensure_ascii=False)

        raise TypeError("Can't convert value to json: %r" % value)

    cpdef object decode(self, object value):
        value = json.loads(value, parse_float=Decimal)
        if self._object:
            return self._object(value)
        elif self._list:
            return [self._list(v) for v in value]
        else:
            return value


cdef class CompositeType(PostgreType):
    cdef EntityType entity

    cpdef object encode(self, object value):
        return value

    cpdef object decode(self, object value):
        return value


cdef class ArrayType(PostgreType):
    cdef PostgreType item_type

    cpdef object encode(self, object value):
        if value is None:
            return None

        return [self.item_type.encode(item) for item in value]

    cpdef object decode(self, object value):
        if value is None:
            return None

        return [self.item_type.decode(item) for item in value]


cdef class PointType(PostgreType):
    cpdef object encode(self, object value):
        return value

    cpdef object decode(self, object value):
        return value


cdef class PostGISGeometryType(PostgreType):
    def __init__(self, str name, srid):
        typestr = f"geometry({name}, {srid})" if srid else f"geometry({name})"
        PostgreType.__init__(self, typestr)


cdef class PostGISGeographyType(PostgreType):
    def __init__(self, str name, srid):
        typestr = f"geography({name}, {srid})" if srid else f"geography({name})"
        PostgreType.__init__(self, typestr)


cdef class PostGISPointType(PostGISGeometryType):
    cpdef object encode(self, object value):
        return value

    cpdef object decode(self, object value):
        return value


cdef class PostGISLatLngType(PostGISGeographyType):
    cpdef object encode(self, object value):
        return value

    cpdef object decode(self, object value):
        return value

