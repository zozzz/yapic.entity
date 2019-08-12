import operator
import cython

from cpython.object cimport PyObject
from cpython.tuple cimport PyTuple_New, PyTuple_GET_ITEM, PyTuple_SET_ITEM, PyTuple_GET_SIZE


cdef extern from "Python.h":
    int _Py_HashPointer(void* ptr)


cdef class Expression:
    cpdef visit(self, Visitor visitor):
        raise NotImplementedError("%s::visit", type(self))

    def __hash__(self): return _Py_HashPointer(<PyObject*>(<object>self))
    def __lt__(Expression self, other): return self._new_binary_expr(other, operator.__lt__)
    def __le__(Expression self, other): return self._new_binary_expr(other, operator.__le__)
    def __eq__(Expression self, other): return self._new_binary_expr(other, operator.__eq__)
    def __ne__(Expression self, other): return self._new_binary_expr(other, operator.__ne__)
    def __ge__(Expression self, other): return self._new_binary_expr(other, operator.__ge__)
    def __gt__(Expression self, other): return self._new_binary_expr(other, operator.__gt__)

    def __add__(Expression self, other): return self._new_binary_expr(other, operator.__add__)
    def __sub__(Expression self, other): return self._new_binary_expr(other, operator.__sub__)
    def __and__(Expression self, other): return self._new_binary_expr(other, operator.__and__)
    def __or__(Expression self, other): return self._new_binary_expr(other, operator.__or__)
    def __xor__(Expression self, other): return self._new_binary_expr(other, operator.__xor__)
    def __invert__(Expression self): return UnaryExpression(self, operator.__invert__)
    def __lshift__(Expression self, other): return self._new_binary_expr(other, operator.__lshift__)
    def __rshift__(Expression self, other): return self._new_binary_expr(other, operator.__rshift__)
    def __mod__(Expression self, other): return self._new_binary_expr(other, operator.__mod__)
    def __mul__(Expression self, other): return self._new_binary_expr(other, operator.__mul__)
    def __truediv__(Expression self, other): return self._new_binary_expr(other, operator.__truediv__)
    def __neg__(Expression self): return UnaryExpression(self, operator.__neg__)
    def __pos__(Expression self): return UnaryExpression(self, operator.__pos__)
    def __pow__(Expression self, other, modulo): return self._new_binary_expr(other, operator.__pow__)
    def __abs__(Expression self): return UnaryExpression(self, operator.__abs__)
    def __call__(Expression self, *args): return CallExpression(self, args)
    def in_(Expression self, *values):
        if len(values) == 1 and isinstance(values[0], (list, tuple, RawExpression, ConstExpression)):
            values = values[0]
        return self._new_binary_expr(values, in_)
    def is_true(Expression self): return self == True
    def is_false(Expression self): return self == False
    def is_null(Expression self): return self == None
    def is_none(Expression self): return self == None
    def startswith(Expression self, other): return self._new_binary_expr(other, startswith)
    def endswith(Expression self, other): return self._new_binary_expr(other, endswith)
    def contains(Expression self, other): return self._new_binary_expr(other, contains)
    def find(Expression self, other): return self._new_binary_expr(other, find)

    cpdef asc(Expression self): return DirectionExpression(self, True)
    cpdef desc(Expression self): return DirectionExpression(self, False)
    cpdef cast(Expression self, str to): return CastExpression(self, to)
    cpdef alias(Expression self, str alias): return AliasExpression(self, alias)

    cdef BinaryExpression _new_binary_expr(Expression self, object other, object op):
        return BinaryExpression(self, other, op)

    def __repr__(self):
        return "<Expr EMPTY>"


cdef class BinaryExpression(Expression):
    def __init__(self, left, right, op):
        self.left = coerce_expression(left)
        self.right = coerce_expression(right)
        self.op = op
        self.negated = False

    def __repr__(self):
        return "<Expr %r %s%r %r>" % (self.left, ("NOT " if self.negated else ""), self.op, self.right)

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

    cdef BinaryExpression _new_binary_expr(self, object other, object op):
        return BinaryExpression(self, other, op)

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


cdef class DirectionExpression(Expression):
    def __cinit__(self, Expression expr, bint is_asc):
        self.expr = expr
        self.is_asc = is_asc

    def __repr__(self):
        return "<Direction %s %s>" % (self.expr, "ASC" if self.is_asc else "DESC")

    cpdef visit(self, Visitor visitor):
        return visitor.visit_direction(self)

    cpdef asc(self):
        return DirectionExpression(self.expr, True)

    cpdef desc(self):
        return DirectionExpression(self.expr, False)


cdef class CallExpression(Expression):
    def __cinit__(self, Expression callable, args):
        self.callable = callable
        self.args = tuple(map(coerce_expression, args))

    def __repr__(self):
        if self.args:
            return "<Call %s( %s )>" % (self.callable, self.args)
        else:
            return "<Call %s()>" % (self.callable)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_call(self)


cdef class RawExpression(Expression):
    def __cinit__(self, str expr):
        self.expr = expr

    def __repr__(self):
        return "<RAW %r>" % (self.expr)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_raw(self)


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


cdef class PathExpression(Expression):
    def __cinit__(self, list path):
        self._path_ = path

    # @cython.wraparound(True)
    def __getattr__(self, object key):
        last_item = self._path_[len(self._path_) - 1]
        new_path = list(self._path_)
        if isinstance(last_item, Expression):
            obj = getattr(last_item, key)
            if isinstance(obj, VirtualExpressionVal):
                return VirtualExpressionVal((<VirtualExpressionVal>obj)._virtual_, self)
            elif isinstance(obj, PathExpression) and last_item is (<PathExpression>obj)._path_[0]:
                new_path.extend((<PathExpression>obj)._path_[1:])
            else:
                new_path.append(obj)
        else:
            new_path.append(key)
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


cdef class VirtualExpressionVal(Expression):
    def __cinit__(self, object virtual, object src):
        self._virtual_ = virtual
        self._source_ = src

    cpdef Expression _create_expr_(self, object q):
        if self._virtual_._val:
            return self._virtual_._val(self._source_, q)
        else:
            raise ValueError("Virtual attribute is not define value expression: %r" % self._virtual_)

    cdef BinaryExpression _new_binary_expr(self, object right, object op):
        return VirtualExpressionBinary(self, right, op)

    cpdef asc(VirtualExpressionVal self):
        return VirtualExpressionDir(self, asc)

    cpdef desc(VirtualExpressionVal self):
        return VirtualExpressionDir(self, desc)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_vexpr_val(self)

    def __getattr__(self, key):
        return getattr(self._virtual_, key)

    def __repr__(self):
        return "<VirtualVal %r :: %r>" % (self._source_, self._virtual_)


cdef class VirtualExpressionBinary(BinaryExpression):
    def __init__(self, Expression left, object right, object op):
        super().__init__(left, right, op)

    @property
    def _virtual_(self):
        if isinstance(self.left, VirtualExpressionVal):
            return (<VirtualExpressionVal>self.left)._virtual_
        elif isinstance(self.left, VirtualExpressionBinary):
            return (<VirtualExpressionBinary>self.left)._virtual_
        else:
            raise TypeError("Something went wrong...")

    cpdef Expression _create_expr_(self, object q):
        if self.op in (operator.__and__, operator.__or__):
            return BinaryExpression(self.left, self.right, self.op)

        if self._virtual_._cmp:
            if isinstance(self.right, ConstExpression):
                value = (<ConstExpression>self.right).value
            else:
                value = self.right

            return self._virtual_._cmp(self.left._source_, q, self.op, value)
        elif self._virtual_._val:
            return self.op(self._virtual_._val(self.left._source_, q), self.right)
        else:
            raise ValueError("Virtual attribute is not define value expression: %r" % self._virtual_)

    cdef BinaryExpression _new_binary_expr(self, object right, object op):
        return VirtualExpressionBinary(self, right, op)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_vexpr_binary(self)

    def __getattr__(self, key):
        return getattr(self.left, key)

    def __repr__(self):
        return "<VirtualBinaryExpr %r %r %r>" % (self.left, self.op, self.right)


cdef class VirtualExpressionDir(Expression):
    def __cinit__(self, object expr, object op):
        self.expr = expr
        self.op = op

    cpdef Expression _create_expr_(self, object q):
        if self.expr._virtual_._order:
            return self.expr._virtual_._order(self.expr._source_, q, self.op)
        elif self.expr._virtual_._val:
            return self.op(self.expr._virtual_._val(self.expr._source_, q, self.op))
        else:
            raise ValueError("Virtual attribute is not define order or value expression: %r" % self.expr._virtual_)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_vexpr_dir(self)

    cpdef asc(VirtualExpressionDir self):
        return VirtualExpressionDir(self.expr, asc)

    cpdef desc(VirtualExpressionDir self):
        return VirtualExpressionDir(self.expr, desc)

    def __repr__(self):
        return "<VirtualDir %r %r>" % (self.expr, self.op)


# cdef class VirtualExpression(BinaryExpression):
#     def __init__(self, object attr, Expression left, Expression right, object op):
#         self._attr = attr
#         super().__init__(left, right, op)

#     cpdef visit(self, Visitor visitor):
#         if self.left._cmp:
#             if not self._cached:
#                 if isinstance(self.right, ConstExpression):
#                     value = (<ConstExpression>self.right).value
#                 else:
#                     value = self.right

#                 self._cached = self.left._cmp(self.left._entity_, None, self.op, value)
#             return visitor.visit(self._cached)
#         else:
#             raise ValueError("Compare expression is not defined for: %r" % self.left)

# cdef class GetAttrExprisson(Expression):
#     def __cinit__(self, Expression obj, object name):
#         self.obj = obj
#         self.name = name

#     def __getattr__(self, name):
#         if isinstance(self.name, str):
#             return GetAttrExprisson(self, name)
#         else:
#             return getattr(self.name, name)

#     cpdef visit(self, Visitor visitor):
#         return visitor.visit_getattr(self)


# cdef class GetItemExprisson(Expression):
#     def __cinit__(self, Expression obj, object index):
#         self.obj = obj
#         self.index = index

#     cpdef visit(self, Visitor visitor):
#         return visitor.visit_getitem(self)


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


cdef Expression coerce_expression(object expr):
    if isinstance(expr, Expression):
        return <Expression>expr
    else:
        if isinstance(expr, list) or isinstance(expr, tuple):
            expr = tuple(coerce_expression(x) for x in expr)
        elif isinstance(expr, str) and (<str>expr).isspace():
            return RawExpression(f"'{expr}'")
        return ConstExpression(expr, type(expr))


def and_(*expr):
    if not expr:
        raise ValueError("Expression must not be empty")

    res = expr[0]

    for i in range(1, len(expr)):
        res &= expr[i]

    return res


def or_(*expr):
    if not expr:
        raise ValueError("Expression must not be empty")

    res = expr[0]

    for i in range(1, len(expr)):
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

cpdef raw(str expr):
    return RawExpression(expr)


cdef class RawIdFactory:
    def __getattribute__(self, name):
        return RawExpression(name)


func = RawIdFactory()
const = RawIdFactory()
