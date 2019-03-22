import typing
import cython
from cpython.object cimport PyObject
from libcpp.memory cimport unique_ptr, make_unique

cdef Typing* typing_ = new Typing()
typing_.Init(typing)


cdef bint is_forward_decl(object o):
    return typing_.IsForwardDecl(o)

cdef tuple get_type_hints(object t):
    return typing_.TypeHints(t)


@cython.final
cdef class Factory:
    @staticmethod
    cdef Factory create(object t):
        cdef bint has_forward_ref = False

        if is_forward_decl(t):
            has_forward_ref = True
            if (<ForwardDecl*>(<PyObject*>t)).IsGeneric():
                t = (<ForwardDecl*>(<PyObject*>t)).Value()
            else:
                return None

        cdef tuple tinfo = typing_.TypeHints(t)
        cdef Factory result

        result = Factory(t, tinfo)
        result.has_forward_ref = has_forward_ref
        return result

    def __cinit__(self, object orig_type, tuple hints):
        self.orig_type = orig_type
        self.hints = hints

    def __call__(self):
        return self.invoke()

    cdef object invoke(self):
        return new_instance(self.orig_type, self.hints, not self.has_forward_ref)


cdef object new_instance(object type, tuple hints, bint resolve_forward):
    cdef object cls
    cdef dict attrs
    cdef tuple init
    cdef tuple arg

    (cls, attrs, init) = hints

    args = []

    if init is not None and init[0] is not None:
        # print(init[0])
        for arg in init[0]:
            argName = arg[0]
            argType = arg[1]
            if argType is not None:
                if is_forward_decl(argType):
                    if resolve_forward:
                        args.append(new_instance_from_forward(argType))
                    else:
                        args.append(argType)
                else:
                    if hasattr(argType, "__origin__") and issubclass(argType.__origin__, typing.Type):
                        args.append(argType.__args__[0])
                    else:
                        hints = typing_.TypeHints(argType)
                        args.append(new_instance(argType, hints, resolve_forward))
            else:
                raise TypeError("Positional arguments must have a type hint: %r" % type)

    return type(*args)


cdef object new_instance_from_forward(object fwd):
    cdef ForwardDecl* forward = (<ForwardDecl*>(<PyObject*>fwd))
    argType = forward.Resolve()
    hints = typing_.TypeHints(argType)
    return new_instance(argType, hints, True)
