import cython

from ._entity cimport EntityType
from ._registry cimport Registry, ScopeDict


@cython.final
cdef class ResolveContext:
    cdef Registry registry
    cdef dict locals
    cdef dict globals
    cdef ScopeDict forward_def
    cdef EntityType entity

    cdef object forward_ref(self, object forward_ref)
    cdef object add_forward(self, EntityType entity)
    cdef object eval(self, str expr, dict locals)
    cdef object _compile(self, str expr)
    cdef object _eval(self, object code, dict locals)
    cdef object _fast_path(self, str expr, dict locals)
    cdef ScopeDict _get_globals(self)

    cdef object get_item(self, key)



