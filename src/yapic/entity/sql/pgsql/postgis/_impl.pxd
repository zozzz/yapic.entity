from yapic.entity._field_impl cimport CompositeImpl


cdef class PostGISImpl(CompositeImpl):
    cdef readonly int srid

cdef class PostGISPointImpl(PostGISImpl):
    pass

cdef class PostGISLatLngImpl(PostGISImpl):
    pass
