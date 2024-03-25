import operator
import cython

from cpython.object cimport PyObject
from cpython.tuple cimport PyTuple_New, PyTuple_GET_ITEM, PyTuple_SET_ITEM, PyTuple_GET_SIZE
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.ref cimport Py_DECREF, Py_INCREF, Py_XDECREF, Py_XINCREF


cdef extern from "Python.h":
    int _Py_HashPointer(void* ptr)


cdef class Expression:
    cpdef visit(self, Visitor visitor):
        raise NotImplementedError("%s::visit", type(self))

    def __hash__(self): return _Py_HashPointer(<PyObject*>(<object>self))
    def __lt__(Expression self, other): return self._new_binary_expr(self, other, operator.__lt__)
    def __le__(Expression self, other): return self._new_binary_expr(self, other, operator.__le__)
    def __eq__(Expression self, other): return self._new_binary_expr(self, other, operator.__eq__)
    def __ne__(Expression self, other): return self._new_binary_expr(self, other, operator.__ne__)
    def __ge__(Expression self, other): return self._new_binary_expr(self, other, operator.__ge__)
    def __gt__(Expression self, other): return self._new_binary_expr(self, other, operator.__gt__)

    def __add__(Expression self, other): return self._new_binary_expr(self, other, operator.__add__)
    def __radd__(Expression self, other): return self._new_binary_expr(other, self, operator.__add__)
    def __sub__(Expression self, other): return self._new_binary_expr(self, other, operator.__sub__)
    def __rsub__(Expression self, other): return self._new_binary_expr(other, self, operator.__sub__)
    def __and__(Expression self, other): return self._new_binary_expr(self, other, operator.__and__)
    def __rand__(Expression self, other): return self._new_binary_expr(other, self, operator.__and__)
    def __or__(Expression self, other): return self._new_binary_expr(self, other, operator.__or__)
    def __ror__(Expression self, other): return self._new_binary_expr(other, self, operator.__or__)
    def __xor__(Expression self, other): return self._new_binary_expr(self, other, operator.__xor__)
    def __rxor__(Expression self, other): return self._new_binary_expr(other, self, operator.__xor__)
    def __lshift__(Expression self, other): return self._new_binary_expr(self, other, operator.__lshift__)
    def __rlshift__(Expression self, other): return self._new_binary_expr(other, self, operator.__lshift__)
    def __rshift__(Expression self, other): return self._new_binary_expr(self, other, operator.__rshift__)
    def __rrshift__(Expression self, other): return self._new_binary_expr(other, self, operator.__rshift__)
    def __mod__(Expression self, other): return self._new_binary_expr(self, other, operator.__mod__)
    def __rmod__(Expression self, other): return self._new_binary_expr(other, self, operator.__mod__)
    def __mul__(Expression self, other): return self._new_binary_expr(self, other, operator.__mul__)
    def __rmul__(Expression self, other): return self._new_binary_expr(other, self, operator.__mul__)
    def __truediv__(Expression self, other): return self._new_binary_expr(self, other, operator.__truediv__)
    def __rtruediv__(Expression self, other): return self._new_binary_expr(other, self, operator.__truediv__)
    def __invert__(Expression self): return UnaryExpression(self, operator.__invert__)
    def __neg__(Expression self): return UnaryExpression(self, operator.__neg__)
    def __pos__(Expression self): return UnaryExpression(self, operator.__pos__)
    def __pow__(Expression self, other, modulo): return self._new_binary_expr(self, other, operator.__pow__)
    def __abs__(Expression self): return UnaryExpression(self, operator.__abs__)
    def __call__(Expression self, *args): return CallExpression(self, args)
    def in_(Expression self, *values):
        if len(values) == 1:
            if isinstance(values[0], (list, tuple, RawExpression, ConstExpression)):
                values = values[0]
            elif isinstance(values[0], set):
                values = list(values[0])

            if isinstance(values, (list, tuple)) and len(values) == 1:
                return self._new_binary_expr(self, values[0], operator.__eq__)
        return self._new_binary_expr(self, values, in_)
    def is_true(Expression self): return self == True
    def is_false(Expression self): return self == False
    def is_null(Expression self): return self == None
    def is_none(Expression self): return self == None
    def startswith(Expression self, other): return self._new_binary_expr(self, other, startswith)
    def endswith(Expression self, other): return self._new_binary_expr(self, other, endswith)
    def contains(Expression self, other): return self._new_binary_expr(self, other, contains)
    def find(Expression self, other): return self._new_binary_expr(self, other, find)

    cpdef asc(Expression self): return OrderExpression(self, True)
    cpdef desc(Expression self): return OrderExpression(self, False)
    cpdef cast(Expression self, str to): return CastExpression(self, to)
    cpdef alias(Expression self, str alias): return AliasExpression(self, alias)
    cpdef over(Expression self): return OverExpression(self)

    cdef BinaryExpression _new_binary_expr(Expression self, object left, object right, object op):
        return BinaryExpression(left, right, op)

    def __repr__(self):
        return "<Expr EMPTY>"


cdef class BinaryExpression(Expression):
    def __init__(self, left, right, op):
        self.left = coerce_expression(left)
        self.right = coerce_expression(right)
        self.op = op
        self.negated = False

    def __repr__(self):
        return "<BinaryExpr %r %s%r %r>" % (self.left, ("NOT " if self.negated else ""), self.op, self.right)

    # TODO: maybe need to clone current + handle child classes as well
    def __invert__(BinaryExpression self):
        op = self.op
        if op is operator.__lt__:
            op = operator.__ge__
        elif op is operator.__le__:
            op = operator.__gt__
        elif op is operator.__eq__:
            op = operator.__ne__
        elif op is operator.__ne__:
            op = operator.__eq__
        elif op is operator.__ge__:
            op = operator.__lt__
        elif op is operator.__gt__:
            op = operator.__le__
        else:
            self.negated = not self.negated
            return self

        self.negated = False
        self.op = op
        return self

    cdef BinaryExpression _new_binary_expr(self, object left, object right, object op):
        return type(self)(left, right, op)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_binary(self)


cdef class UnaryExpression(Expression):
    def __cinit__(self, expr, op):
        self.expr = coerce_expression(expr)
        self.op = op

    def __repr__(self):
        return "<Expr %r%r>" % (self.op, self.expr)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_unary(self)


cdef class CastExpression(Expression):
    def __cinit__(self, expr, to):
        self.expr = coerce_expression(expr)
        self.to = to

    def __repr__(self):
        return "<Cast %r to %r>" % (self.expr, self.to)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_cast(self)

    cpdef cast(self, str to):
        return CastExpression(self.expr, to)


cdef class ConstExpression(Expression):
    def __cinit__(self, object value, type type):
        self.value = value
        self.type = type

    def __repr__(self):
        return "<Const %s typeof %s>" % (self.value, self.type.__name__)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_const(self)

    cpdef alias(self, str alias):
        return self


cdef class OrderExpression(Expression):
    def __cinit__(self, Expression expr, bint is_asc):
        self.expr = expr
        self.is_asc = is_asc

    def __repr__(self):
        return "<Order %s %s>" % (self.expr, "ASC" if self.is_asc else "DESC")

    cpdef visit(self, Visitor visitor):
        return visitor.visit_order(self)

    cpdef asc(self):
        return type(self)(self.expr, True)

    cpdef desc(self):
        return type(self)(self.expr, False)


cdef class CallExpression(Expression):
    def __cinit__(self, Expression callable, tuple args):
        self.callable = callable
        self.args = coerce_expr_list(args)

    def __repr__(self):
        if self.args:
            return "<Call %s( %s )>" % (self.callable, self.args)
        else:
            return "<Call %s()>" % (self.callable)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_call(self)


cdef class RawExpression(Expression):
    def __cinit__(self, *exprs):
        self.exprs = exprs

    def __repr__(self):
        return "<RAW %r>" % (self.exprs)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_raw(self)


cdef class ParamExpression(Expression):
    def __cinit__(self, value):
        self.value = value

    def __repr__(self):
        return "<PARAM %r>" % (self.value)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_param(self)



cdef class AliasExpression(Expression):
    def __cinit__(self, Expression expr, str alias):
        self.expr = expr
        self.value = alias

    def __repr__(self):
        return "<Alias %s AS %s>" % (self.expr, self.value)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_alias(self)

    cpdef alias(self, str alias):
        return AliasExpression(self.expr, alias)


cdef class ColumnRefExpression(Expression):
    def __cinit__(self, Expression expr, int index):
        self.expr = expr
        self.index = index

    def __repr__(self):
        return "<ColumnRef %s idx=%s>" % (self.expr, self.index)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_column_ref(self)


cdef class PathExpression(Expression):
    def __cinit__(self, list path):
        self._path_ = path

    def __getattr__(self, object key):
        from ._relation import Relation

        last_item = self._path_[len(self._path_) - 1]
        new_path = list(self._path_)
        if isinstance(last_item, Expression):
            obj = getattr(last_item, key)
            if isinstance(obj, PathExpression):
                new_first = (<PathExpression>obj)._path_[0]
                if last_item is new_first:
                    new_path.extend((<PathExpression>obj)._path_[1:])
                elif isinstance(new_first, Relation) and not isinstance(last_item, Relation):
                    new_path.pop()
                    new_path.extend((<PathExpression>obj)._path_)
                else:
                    raise NotImplementedError(str(obj))
            else:
                new_path.append(obj)
        else:
            if hasattr(last_item, key):
                new_path.append(key)
            else:
                raise AttributeError(f"{last_item} has no attribute '{key}'")
        return PathExpression(new_path)

    # @cython.wraparound(True)
    def __getitem__(self, object key):
        last_item = self._path_[len(self._path_) - 1]
        new_path = list(self._path_)
        if isinstance(last_item, Expression):
            obj = last_item[key]
            if isinstance(obj, PathExpression) and last_item is (<PathExpression>obj)._path_[0]:
                new_path.extend((<PathExpression>obj)._path_[1:])
            else:
                new_path.append(obj)
        else:
            new_path.append(key)
        return PathExpression(new_path)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_path(self)

    def __repr__(self):
        return "<Path %r>" % self._path_


cdef class OverExpression(Expression):
    def __cinit__(self, Expression expr):
        self.expr = expr
        self._order = []
        self._partition = []

    def order(self, *expr):
        for item in expr:
            if isinstance(item, (OrderExpression, RawExpression)):
                self._order.append(item)
            elif isinstance(item, Expression):
                self._order.append((<Expression>item).asc())
            else:
                raise ValueError("Invalid value for order: %r" % item)
        return self

    def partition(self, *expr):
        for item in expr:
            self._partition.append(item)
        return self

    cpdef visit(self, Visitor visitor):
        return visitor.visit_over(self)

    def __repr__(self):
        return "<%r Over ORDER: %r, PARTITION: %r>" % (self.expr, self._order, self._partition)


@cython.final
cdef class ExpressionPlaceholder(Expression):
    def __cinit__(self, object origin, str name):
        self.origin = origin
        self.path = []
        self.name = name

    def __getattr__(self, key):
        self.origin = getattr(self.origin, key)
        self.path.append((EPA.GETATTR, key))
        return self

    def __getitem__(self, key):
        self.origin = self.origin[key]
        self.path.append((EPA.GETITEM, key))
        return self

    def __call__(self, *args, **kwargs):
        self.origin = self.origin(*args, **kwargs)
        self.path.append((EPA.CALL, (args, kwargs)))
        return self

    cpdef visit(self, Visitor visitor):
        return visitor.visit_placeholder(self)

    cdef Expression eval(self, object origion):
        cdef list path = self.path
        cdef object result = origion
        cdef EPA op

        for entry in self.path:
            op = (<tuple>entry)[0]

            if op == EPA.GETATTR:
                result = getattr(result, (<tuple>entry)[1])
            elif op == EPA.GETITEM:
                result = result[(<tuple>entry)[1]]
            elif op == EPA.CALL:
                result = result(*(<tuple>entry)[1], **(<tuple>entry)[2])
            else:
                raise RuntimeError(f"Uknown operation: {op}")

        return coerce_expression(result)

    def __repr__(self):
        return f"<Placeholder {self.name} {self.path}>"


# TODO: invert
# @cython.final
# cdef class MultiExpression(Expression):
#     def __cinit__(self, tuple expressions, object combinator):
#         self.expressions = expressions
#         self.combinator = combinator

#     cpdef visit(self, Visitor visitor):
#         return visitor.visit_multi(self)

#     cdef BinaryExpression _new_binary_expr(MultiExpression self, object left, object right, object op):
#         cdef list parts = []

#         if isinstance(right, tuple):
#             if len(<tuple>right) != len(self.expressions):
#                 raise ValueError(f"Wrong count of paramaters ({right}) for multi expression {self}")
#             else:
#                 for i, expr in enumerate(self.expressions):
#                     parts.append(op(expr, (<tuple>right)[i]))
#         else:
#             for i, expr in enumerate(self.expressions):
#                 parts.append(op(expr, right))

#         return self.combinator(*parts)

#     def __repr__(self):
#         return f"<Multi {self.combinator} {self.expressions}>"


cdef class Visitor:
    cpdef visit(self, Expression expr):
        return expr.visit(self)

    def visit_binary(self, binary):
        if binary.op is operator.__or__:
            fn_name = "visit_binary_or"
        elif binary.op is operator.__and__:
            fn_name = "visit_binary_and"
        elif binary.op is in_:
            fn_name = "visit_binary_in"
        else:
            fn_name = f"visit_binary_{binary.op.__name__}"
        return getattr(self, fn_name)(binary)

    def visit_unary(self, unary):
        fn_name = f"visit_unary_{unary.op.__name__}"
        return getattr(self, fn_name)(unary)

    def _visit_iterable(self, expr):
        if isinstance(expr, tuple):
            return self._visit_tuple(expr)
        elif isinstance(expr, list):
            return self._visit_list(expr)
        else:
            return self._visit_any_iterable(expr)

    def _visit_list(self, list expr):
        cdef int length = len(expr)
        cdef list result = PyList_New(length)
        cdef object item

        for i in range(0, length):
            item = self.visit(<object>(<PyObject*>(<list>expr)[i]))
            Py_INCREF(item)
            PyList_SET_ITEM(result, i, item)

        return result

    def _visit_tuple(self, tuple expr):
        cdef int length = len(expr)
        cdef tuple result = PyTuple_New(length)
        cdef object item

        for i in range(0, length):
            item = self.visit(<object>(<PyObject*>(<tuple>expr)[i]))
            Py_INCREF(item)
            PyTuple_SET_ITEM(result, i, item)

        return result

    def _visit_any_iterable(self, object expr):
        cdef list res = []
        for expr in expr:
            res.append(self.visit(expr))
        return res

    def __getattr__(self, key):
        if key.startswith("visit_"):
            return self.__default__
        else:
            raise AttributeError(key)

    def __default__(self, object expr):
        raise NotImplementedError(f"visit expression {expr} in {self} visitor")



cdef Expression coerce_expression(object expr):
    if isinstance(expr, Expression):
        return <Expression>expr
    else:
        if isinstance(expr, (list, tuple)):
            expr = coerce_expr_list(expr)
        elif isinstance(expr, str) and (<str>expr).isspace():
            return RawExpression(f"'{expr}'")
        return ConstExpression(expr, type(expr))


cdef tuple coerce_expr_list(object expr):
    cdef int length = len(expr)
    cdef tuple result = PyTuple_New(length)
    cdef object item

    if length > 0:
        if isinstance(expr, list):
            for i in range(0, length):
                item = coerce_expression(<object>(<PyObject*>(<list>expr)[i]))
                Py_INCREF(item)
                PyTuple_SET_ITEM(result, i, item)
        elif isinstance(expr, tuple):
            for i in range(0, length):
                item = coerce_expression(<object>(<PyObject*>(<tuple>expr)[i]))
                Py_INCREF(item)
                PyTuple_SET_ITEM(result, i, item)
        else:
            raise ValueError(f"Unexpected expression list: {expr}")

    return result



def and_(*expr):
    cdef int length = len(expr)

    if length == 0:
        raise ValueError("Expression must not be empty")

    res = expr[0]
    for i in range(1, length):
        res &= expr[i]
    return res


def or_(*expr):
    cdef int length = len(expr)

    if length == 0:
        raise ValueError("Expression must not be empty")

    res = expr[0]
    for i in range(1, length):
        res |= expr[i]
    return res


def in_(expr, *value):
    return expr.in_(*value)

def startswith(expr, value):
    return expr.startswith(value)

def endswith(expr, value):
    return expr.endswith(value)

def contains(expr, value):
    return expr.contains(value)

def find(expr, value):
    return expr.find(value)

def asc(expr):
    return expr.asc()

def desc(expr):
    return expr.desc()

cpdef direction(Expression expr, str dir):
    if str(dir).lower() == "asc":
        return expr.asc()
    else:
        return expr.desc()

def raw(*exprs):
    return RawExpression(*exprs)

def param(value):
    return ParamExpression(value)

cdef class RawIdFactory:
    def __getattribute__(self, name):
        return RawExpression(name)


func = RawIdFactory()
const = RawIdFactory()
