import operator
import pytest

from yapic.entity._expression import Expression, BinaryExpression, UnaryExpression, ConstExpression, Visitor
from yapic.entity._field import Field


def test_basics():
    e = Expression()
    assert isinstance(e, Expression)


def test_const_expression():
    e = Expression() + 42
    assert isinstance(e, Expression)
    assert isinstance(e, BinaryExpression)
    assert isinstance(e.left, Expression)
    assert isinstance(e.right, ConstExpression)
    assert e.right.value == 42
    assert e.right.type == int


def test_unary_expression():
    e = ~Expression()
    assert isinstance(e, Expression)
    assert isinstance(e, UnaryExpression)
    assert isinstance(e.expr, Expression)
    assert e.op == operator.__invert__


@pytest.mark.parametrize(
    "op", [
        operator.__lt__,
        operator.__le__,
        operator.__eq__,
        operator.__ne__,
        operator.__ge__,
        operator.__gt__,
        operator.__add__,
        operator.__sub__,
        operator.__and__,
        operator.__or__,
        operator.__xor__,
        operator.__lshift__,
        operator.__rshift__,
        operator.__mod__,
        operator.__mul__,
        operator.__truediv__,
        operator.__pow__,
    ],
    ids=lambda v: v.__name__)
def test_binary_expression(op):
    a = Expression()
    b = Expression()

    e = op(a, b)
    assert isinstance(e, Expression)
    assert isinstance(e, BinaryExpression)
    assert isinstance(e.left, Expression)
    assert isinstance(e.right, Expression)
    assert e.op is op
    assert e.left is a
    assert e.right is b


def test_in_expression():
    a = Expression()
    b = Expression()

    e = a.in_(b)
    assert isinstance(e, Expression)
    assert isinstance(e, BinaryExpression)
    assert isinstance(e.left, Expression)
    assert isinstance(e.right, Expression)
    assert e.op is operator.__contains__
    assert e.left is a
    assert e.right is b


def test_field_expression():
    f = Field()

    e = f + 2
    assert isinstance(e, Expression)
    assert isinstance(e, BinaryExpression)
    assert isinstance(e.left, Field)
    assert isinstance(e.right, ConstExpression)
    assert e.op is operator.__add__
    assert e.left is f
    assert e.right == 2


def test_visitor():
    class MyVisitor(Visitor):
        def __init__(self):
            super().__init__()
            self.compiled = []

        def visit_binary_eq(self, binary):
            self.visit(binary.left)
            self.compiled.append(" == ")
            self.visit(binary.right)

        def visit_binary_or(self, binary):
            self.compiled.append("(")
            self.visit(binary.left)
            self.compiled.append(") OR (")
            self.visit(binary.right)
            self.compiled.append(")")

        def visit_const(self, const):
            if const.type is int:
                self.compiled.append(str(const.value))
            else:
                raise ValueError()

        def visit_field(self, field):
            self.compiled.append(field.name)

    visitor = MyVisitor()

    a = (Field(name="field1") == 1) | (Field(name="field2") == 2)
    a.visit(visitor)

    assert "".join(visitor.compiled) == "(field1 == 1) OR (field2 == 2)"
