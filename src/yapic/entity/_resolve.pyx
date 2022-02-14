import re
import cython

from cpython.object cimport PyObject
from cpython.weakref cimport PyWeakref_GetObject

from ._entity cimport EntityType
from ._registry cimport Registry, ScopeDict
from ._expression import and_, or_, func, const
from ._factory cimport new_instance_from_forward


cdef extern from "Python.h":
    cdef int Py_eval_input

    object Py_CompileString(const char *src, const char *filename, int start)
    object PyEval_EvalCode(object code, object globals, object locals)


COMPILE_CACHE = {}
IS_FAST_PATH = re.compile(r"^(?:[a-z_]*\d*)(?:\.[a-z_]*\d*)*$", re.I).match
BUILTINS = {
    "and_": and_,
    "or_": or_,
    "func": func,
    "const": const,
}


@cython.final
cdef class ResolveContext:
    def __cinit__(self, EntityType entity, object frame):
        self.entity = entity
        self.registry = entity.get_registry()
        self.forward_def = ScopeDict()

        cdef dict locals
        cdef dict globals
        if frame is not None:
            self.locals = frame.f_locals
            self.globals = frame.f_globals
        else:
            self.locals = {}
            self.globals = {}

    cdef object forward_ref(self, object forward_ref):
        cdef dict extra = ScopeDict(self.forward_def)
        extra.update(self.registry.locals)
        return new_instance_from_forward(forward_ref, extra)

    cdef object add_forward(self, EntityType entity):
        cdef ResolveContext other = entity.resolve_ctx
        if other is not None:
            if self.globals is other.globals and self.locals is other.locals:
                self.forward_def[entity.__name__] = entity

    cdef object eval(self, str expr, dict locals):
        if IS_FAST_PATH(expr):
            return self._fast_path(expr, locals)
        else:
            return self._eval(self._compile(expr), locals)

    cdef object _compile(self, str expr):
        try:
            return COMPILE_CACHE[expr]
        except KeyError:
            compiled = Py_CompileString(expr.encode("utf-8"), "<string>", Py_eval_input)
            # TODO: lehet mégsem kéne ez ide
            COMPILE_CACHE[expr] = compiled
            return compiled

    cdef object _fast_path(self, str expr, dict locals):
        cdef list parts = expr.split(".")
        cdef object result

        try:
            result = locals[parts[0]]
        except KeyError:
            result = self.get_item(parts[0])

        for i in range(1, len(parts)):
            result = getattr(result, parts[i])

        return result

    cdef object _eval(self, object code, dict locals):
        return <object>PyEval_EvalCode(code, self._get_globals(), locals)

    cdef ScopeDict _get_globals(self):
        cdef ScopeDict result = ScopeDict(BUILTINS)
        result.update(self.forward_def)
        if self.globals is not self.locals:
            result.update(self.globals)
        result.update(self.registry.locals)
        result.update(self.locals)
        result[self.entity.__name__] = self.entity
        return result

    cdef object get_item(self, key):
        cdef Registry registry

        if key == self.entity.__name__:
            return self.entity

        try:
            return self.locals[key]
        except KeyError:
            pass

        try:
            return self.registry.locals[key]
        except KeyError:
            pass

        if self.globals is not self.locals:
            try:
                return self.globals[key]
            except KeyError:
                pass

        try:
            return self.forward_def[key]
        except KeyError:
            pass

        try:
            return BUILTINS[key]
        except KeyError:
            pass

        raise NameError(f"'{key}' not found in resolve context")

    def __getitem__(self, key):
        return self.get_item(key)

    def __getattr__(self, key):
        return self.get_item(key)



