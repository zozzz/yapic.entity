import operator
import pytest

from yapic.sql import PostgreDialect
from yapic.entity import Query, Entity, Serial, String, and_, or_

dialect = PostgreDialect()


class User(Entity):
    id: Serial
    name: String
    email: String
    # birth_date:


def test_query_basics():
    q = Query()
    q.select_from(User)
    q.where(User.id == 42)

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT * FROM "User" as t0 WHERE "t0"."id" = $1'
    assert params == (42, )


def test_query_and_or():
    q = Query()
    q.select_from(User)
    q.where(or_(User.id == 1, User.id == 2, User.id == 3))
    q.where(User.email == "email")
    q.where(or_(and_(User.id == 1, User.id == 2), User.id == 3))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT * FROM "User" as t0 WHERE ("t0"."id" = $1 OR "t0"."id" = $2 OR "t0"."id" = $3) AND "t0"."email" = $4 AND (("t0"."id" = $1 AND "t0"."id" = $2) OR "t0"."id" = $3)'
    assert params == (1, 2, 3, "email")


def test_in():
    q = Query()
    q.select_from(User)
    q.where(User.id.in_(1, 2, 3))
    q.where(User.id.in_([1, 2, 3]))
    q.where(User.id.in_([1, User.email, 3]))
    q.where(User.id.in_(1, User.email, 3))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT * FROM "User" as t0 WHERE "t0"."id" IN ($1, $2, $3) AND "t0"."id" IN ($1, $2, $3) AND "t0"."id" IN ($1, "t0"."email", $3) AND "t0"."id" IN ($1, "t0"."email", $3)'
    assert params == (1, 2, 3)


def test_in_eq():
    q = Query()
    q.select_from(User)
    q.where(User.id.in_(1, 2, 3) == True)

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT * FROM "User" as t0 WHERE "t0"."id" IN ($1, $2, $3) IS TRUE'
    assert params == (1, 2, 3)


def test_is_true():
    q = Query()
    q.select_from(User)
    q.where(User.id.is_true())
    q.where(~User.id.is_true())

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT * FROM "User" as t0 WHERE "t0"."id" IS TRUE AND "t0"."id" IS NOT TRUE'
    assert params == ()


binary_operator_cases = [
    (operator.__eq__, User.email, None, '"t0"."email" IS NULL'),
    (operator.__eq__, User.email, True, '"t0"."email" IS TRUE'),
    (operator.__eq__, User.email, False, '"t0"."email" IS FALSE'),
    (operator.__ne__, User.email, None, '"t0"."email" IS NOT NULL'),
    (operator.__ne__, User.email, True, '"t0"."email" IS NOT TRUE'),
    (operator.__ne__, User.email, False, '"t0"."email" IS NOT FALSE'),
    (operator.__eq__, User.email, 1, '"t0"."email" = $1'),
    (operator.__ne__, User.email, 1, '"t0"."email" != $1'),
    (operator.__lt__, User.email, 1, '"t0"."email" < $1'),
    (operator.__le__, User.email, 1, '"t0"."email" <= $1'),
    (operator.__gt__, User.email, 1, '"t0"."email" > $1'),
    (operator.__ge__, User.email, 1, '"t0"."email" >= $1'),
    (operator.__add__, User.email, 1, '"t0"."email" + $1'),
    (operator.__sub__, User.email, 1, '"t0"."email" - $1'),
    (operator.__lshift__, User.email, 1, '"t0"."email" << $1'),
    (operator.__rshift__, User.email, 1, '"t0"."email" >> $1'),
    (operator.__mod__, User.email, 1, '"t0"."email" % $1'),
    (operator.__mul__, User.email, 1, '"t0"."email" * $1'),
    (operator.__truediv__, User.email, 1, '"t0"."email" / $1'),
    (operator.__pow__, User.email, 1, '"t0"."email" ^ $1'),
]


@pytest.mark.parametrize("op,left,right,expected", binary_operator_cases, ids=[x[3] for x in binary_operator_cases])
def test_binary_operators(op, left, right, expected):
    q = Query()
    q.select_from(User)
    q.where(op(left, right))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT * FROM "User" as t0 WHERE ' + expected


unary_operator_cases = [
    (operator.__abs__, User.email, '@"t0"."email"'),
    (operator.__invert__, User.email, 'NOT "t0"."email"'),
    (operator.__neg__, User.email, '-"t0"."email"'),
    (operator.__pos__, User.email, '+"t0"."email"'),
]


@pytest.mark.parametrize("op,left,expected", unary_operator_cases, ids=[x[2] for x in unary_operator_cases])
def test_unary_operators(op, left, expected):
    q = Query()
    q.select_from(User)
    q.where(op(left))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT * FROM "User" as t0 WHERE ' + expected


invert_operators = [
    (operator.__lt__, "<", ">="),
    (operator.__le__, "<=", ">"),
    (operator.__gt__, ">", "<="),
    (operator.__ge__, ">=", "<"),
    (operator.__eq__, "=", "!="),
    (operator.__ne__, "!=", "="),
]


@pytest.mark.parametrize("op,original,inverted", invert_operators, ids=[f"{x[1]} NOT {x[2]}" for x in invert_operators])
def test_op_invert(op, original, inverted):
    q = Query()
    q.select_from(User)
    q.where(op(User.id, User.email))
    q.where(~op(User.id, User.email))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == f'SELECT * FROM "User" as t0 WHERE "t0"."id" {original} "t0"."email" AND "t0"."id" {inverted} "t0"."email"'
