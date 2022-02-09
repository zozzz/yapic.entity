from yapic.entity._entity cimport EntityType, EntityBase, EntityAttribute
from yapic.entity._entity import Entity
from yapic.entity._field cimport Field, StorageType
from yapic.entity._field_impl cimport FloatImpl, IntImpl, CompositeImpl
from yapic.entity._registry cimport Registry
from yapic.entity._expression cimport RawExpression, CallExpression


POSTGIS_REG = Registry()
DEFAULT_SRID = 4326


cdef class PostGISImpl(CompositeImpl):
    def __init__(self, entity, int srid=0):
        entity.set_meta("is_builtin", True)
        super().__init__(entity)
        self.srid = srid or DEFAULT_SRID

    def __eq__(self, other):
        return CompositeImpl.__eq__(self, other) and self.srid == other.srid


class PostGISPointType(Entity,
                registry=POSTGIS_REG,
                _root=True,
                is_type=True,
                is_builtin=True,
                __fields__ = [
                    Field(FloatImpl(), name="x", size=8),
                    Field(FloatImpl(), name="y", size=8),
                    Field(IntImpl(), name="srid", size=4),
                ]):

    def __init__(self, data=None, **kwargs):
        if isinstance(data, (tuple, list)):
            if len(data) < 2:
                raise ValueError("Specify at least two coordinates")
            data = dict(x=data[0], y=data[1], srid=data[2] if len(data) > 2 else DEFAULT_SRID)
        super().__init__(data, **kwargs)


cdef class PostGISPointImpl(PostGISImpl):
    def __init__(self, int srid=0):
        super().__init__(PostGISPointType, srid)

    cpdef getattr(self, EntityAttribute attr, object key):
        if key == "x":
            return CallExpression(RawExpression("ST_X"), (attr.cast("GEOMETRY"), ))
        elif key == "y":
            return CallExpression(RawExpression("ST_Y"), (attr.cast("GEOMETRY"), ))
        elif key == "srid":
            return CallExpression(RawExpression("ST_SRID"), (attr.cast("GEOMETRY"), ))

    cpdef object data_for_write(self, EntityBase value, bint for_insert):
        result = CallExpression(RawExpression("ST_MakePoint"), (value.x, value.y))
        srid = value.srid or self.srid
        return CallExpression(RawExpression("ST_SetSRID"), (result, srid))

    def __repr__(self):
        return "PostGIS.Point"


class PostGISLatLngType(Entity,
                registry=POSTGIS_REG,
                _root=True,
                is_type=True,
                is_builtin=True,
                __fields__ = [
                    Field(FloatImpl(), name="lat", size=8),
                    Field(FloatImpl(), name="lng", size=8),
                    Field(IntImpl(), name="srid", size=4),
                ]):

    def __init__(self, data=None, **kwargs):
        if isinstance(data, (tuple, list)):
            if len(data) < 2:
                raise ValueError("Specify at least two coordinates")
            data = dict(lat=data[0], lng=data[1], srid=data[2] if len(data) > 2 else DEFAULT_SRID)
        super().__init__(data, **kwargs)


cdef class PostGISLatLngImpl(PostGISImpl):
    def __init__(self, int srid=0):
        super().__init__(PostGISLatLngType, srid)

    cpdef getattr(self, EntityAttribute attr, object key):
        if key == "lng":
            return CallExpression(RawExpression("ST_X"), (attr.cast("GEOMETRY"), ))
        elif key == "lat":
            return CallExpression(RawExpression("ST_Y"), (attr.cast("GEOMETRY"), ))
        elif key == "srid":
            return CallExpression(RawExpression("ST_SRID"), (attr.cast("GEOMETRY"), ))

    cpdef object data_for_write(self, EntityBase value, bint for_insert):
        result = CallExpression(RawExpression("ST_MakePoint"), (value.lng, value.lat))
        srid = value.srid or self.srid
        return CallExpression(RawExpression("ST_SetSRID"), (result, srid))

    def __repr__(self):
        return "PostGIS.LatLng"
