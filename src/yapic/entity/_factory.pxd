import cython

from cpython.object cimport PyObject

from ._field import Field
from ._relation import Relation


cdef extern from "yapic/typing.hpp" namespace "Yapic":
    cdef cppclass Typing:
        Typing()
        bint Init(object typingModule) except 0
        bint IsGeneric(object o)
        bint IsGenericType(object o)
        bint IsForwardRef(object o)
        bint IsForwardDecl(object o)
        dict ResolveTypeVars(object t)
        dict ResolveTypeVars(object t, object vars)
        tuple ResolveMro(object t)
        tuple ResolveMro(object t, object vars)
        tuple TypeHints(object t)
        tuple TypeHints(object t, object vars)
        tuple CallableHints(object callable)
        tuple CallableHints(object callable, object bound)
        tuple CallableHints(object callable, object bound, object vars)

    cdef cppclass ForwardDecl:
        bint IsGeneric()
        object Value()
        object Resolve()
        object Resolve(object o)


cdef bint is_forward_decl(object o)

cdef tuple get_type_hints(object t)


ctypedef bint (*set_attribute_fn) (object instance, dict attributes)

@cython.final
cdef class Factory:
    cdef object orig_type
    # cdef set_attribute_fn init_attrs
    cdef tuple hints
    cdef bint has_forward_ref

    @staticmethod
    cdef Factory create(object t)

    cdef object invoke(self)


cdef object new_instance(object type, tuple hints, bint resolve_forward, dict locals)

cdef object new_instance_from_forward(object fwd, dict locals)
