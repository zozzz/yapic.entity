from datetime import date, datetime
from decimal import Decimal

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
    ChoiceImpl,
    JsonImpl,
    CompositeImpl,
    UUIDImpl
)
from yapic.entity._geom_impl cimport (
    PointImpl,
)

from ._dialect cimport PostgreDialect


cdef class PostgreTypeFactory(StorageTypeFactory):
    def __cinit__(self, PostgreDialect dialect):
        self.dialect = dialect

    cpdef StorageType create(self, Field field):
        impl = field._impl_

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
        elif isinstance(impl, CompositeImpl):
            return self.__composite_type(field, <CompositeImpl>impl)

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

    cdef StorageType __numeric_type(self, Field field, NumericImpl impl):
        if field.min_size <= 0 or field.max_size <= 0:
            raise ValueError(f"Invalid values for Numeric type: {(field.min_size, field.max_size)}")

        return NumericType(f"NUMERIC({field.min_size}, {field.max_size})")

    cdef StorageType __float_type(self, Field field, FloatImpl impl):
        return FloatType(f"FLOAT{field.max_size}")

    cdef StorageType __uuid_type(self, Field field, UUIDImpl impl):
        return UUIDType(f"UUID")

    cdef StorageType __choice_type(self, Field field, ChoiceImpl impl):
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

    cdef StorageType __json_type(self, Field field, JsonImpl impl):
        cdef JsonType t = JsonType("JSONB")
        t.entity = impl._entity_
        return t

    cdef StorageType __composite_type(self, Field field, CompositeImpl impl):
        cdef CompositeType t = CompositeType(self.dialect.table_qname(impl._entity_))
        t.entity = impl._entity_
        return t

    cdef StorageType __point_type(self, Field field, PointImpl impl):
        return PointType("POINT")


cdef class PostgreType(StorageType):
    def __cinit__(self, str name, str pre_sql = None, str post_sql = None):
        self.name = name
        self.pre_sql = pre_sql
        self.post_sql = post_sql


cdef class IntType(PostgreType):
    cpdef object encode(self, object value):
        if isinstance(value, int):
            return value
        else:
            return int(value)

    cpdef object decode(self, object value):
        return value


cdef class StringType(PostgreType):
    cpdef object encode(self, object value):
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
        if not isinstance(value, bytes):
            raise ValueError("Bytes type only accepts byte strings")
        return value

    cpdef object decode(self, object value):
        return value


cdef class BoolType(PostgreType):
    cpdef object encode(self, object value):
        return RawExpression("TRUE" if bool(value) else "FALSE")

    cpdef object decode(self, object value):
        if isinstance(value, bool):
            return value
        elif not isinstance(value, str):
            value = str(value)
        return value.lower() in ("true", "t", "1", "y", "yes", "on")


cdef class DateType(PostgreType):
    cpdef object encode(self, object value):
        return value.strftime("%Y-%m-%d")

    cpdef object decode(self, object value):
        if isinstance(value, date):
            return value
        return datetime.strptime(value, "%Y-%m-%d").date()


cdef class DateTimeType(PostgreType):
    cpdef object encode(self, object value):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")

    cpdef object decode(self, object value):
        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")


cdef class DateTimeTzType(PostgreType):
    cpdef object encode(self, object value):
        if value.utcoffset() is None:
            raise ValueError("datetime value must have timezone information")
        return value.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    cpdef object decode(self, object value):
        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f%z")


cdef class NumericType(PostgreType):
    cpdef object encode(self, object value):
        if value is None:
            return None
        return str(value)

    cpdef object decode(self, object value):
        if isinstance(value, str):
            return Decimal(value)
        else:
            return value


cdef class FloatType(PostgreType):
    cpdef object encode(self, object value):
        if isinstance(value, str):
            return float(value)
        else:
            return value

    cpdef object decode(self, object value):
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
    cpdef object encode(self, object value):
        pass

    cpdef object decode(self, object value):
        pass


cdef class JsonType(PostgreType):
    cdef EntityType entity

    cpdef object encode(self, object value):
        if value is None:
            return None
        elif isinstance(value, EntityBase):
            if type(value) is not self.entity:
                raise ValueError("Missmatch entity type: %r expected %r" % (type(value), self.entity))
            return json.dumps(value.as_dict())
        else:
            raise TypeError("Can convert value to json: %r" % value)

    cpdef object decode(self, object value):
        return self.entity(json.loads(value))


cdef class CompositeType(PostgreType):
    cdef EntityType entity

    cpdef object encode(self, object value):
        return value

    cpdef object decode(self, object value):
        return value


cdef class PointType(PostgreType):
    cpdef object encode(self, object value):
        return value

    cpdef object decode(self, object value):
        return value
