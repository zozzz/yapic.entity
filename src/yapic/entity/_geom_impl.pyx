from ._entity cimport EntityType, EntityBase
from ._entity import Entity
from ._field cimport Field
from ._field_impl cimport FloatImpl, NamedTupleImpl
from ._registry cimport Registry


GEOM_REG = Registry()


class PointType(Entity,
                registry=GEOM_REG,
                _root=True,
                is_type=True,
                is_virtual=True,
                __fields__ = [
                    Field(FloatImpl(), name="x", size=8),
                    Field(FloatImpl(), name="y", size=8),
                ]):

    def __init__(self, data=None, **kwargs):
        if isinstance(data, (tuple, list)):
            data = dict(x=data[0], y=data[1])
        super().__init__(data, **kwargs)


cdef class PointImpl(NamedTupleImpl):
    def __init__(self):
        super().__init__(PointType)

    def __repr__(self):
        return "Point"
