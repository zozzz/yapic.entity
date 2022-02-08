from ._entity cimport EntityType, EntityBase
from ._entity import Entity
from ._field cimport Field, StorageType
from ._field_impl cimport FloatImpl, NamedTupleImpl
from ._registry cimport Registry
from ._expression cimport CallExpression, RawExpression

POINT_REGISTRY = Registry()


# TODO ha is_type is True vagy is_builltin akkor nem kell registry + hozzáadható a deps-hez
class PointType(Entity,
                registry=POINT_REGISTRY,
                _root=True,
                is_type=True,
                is_builtin=True,
                __fields__ = [
                    Field(FloatImpl(), name="x", size=8),
                    Field(FloatImpl(), name="y", size=8),
                ]):

    def __init__(self, data=None, **kwargs):
        if isinstance(data, (tuple, list)):
            if len(data) < 2:
                raise ValueError("Missing coordinates")
            data = dict(x=data[0], y=data[1])
        super().__init__(data, **kwargs)


cdef class PointImpl(NamedTupleImpl):
    def __init__(self):
        super().__init__(PointType)

    cpdef object data_for_write(self, EntityBase value, bint for_insert):
        if for_insert:
            return CallExpression(RawExpression("POINT"), (value.x, value.y))
        else:
            return value

    def __repr__(self):
        return "Point"
