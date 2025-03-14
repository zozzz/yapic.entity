# flake8: noqa: E501

import operator
from typing import Any

import pytest

from yapic.entity import (
    Auto,
    Bool,
    Choice,
    Composite,
    DateTimeTz,
    Entity,
    Enum,
    Field,
    ForeignKey,
    Int,
    Json,
    Loading,
    ManyAcross,
    One,
    Query,
    Registry,
    Serial,
    String,
    and_,
    contains,
    endswith,
    find,
    func,
    or_,
    param,
    raw,
    startswith,
    virtual,
)
from yapic.entity.sql import PostgreDialect

dialect = PostgreDialect()


class XYZ(Entity):
    x: Int
    y: Int
    z: Int


class FullName(Entity):
    title: String
    family: String
    given: String
    xyz: Json[XYZ]

    # TODO: @virtual(String, store=True) if store is true this function must return an expression
    @virtual
    def formatted(self):
        return " ".join(filter(bool, (self.title, self.family, self.given)))

    @formatted.compare
    def formatted_compare(cls, q: Query, op: Any, value: Any):
        if op is contains:
            res = []
            parts = value.split()

            for f in (cls.family, cls.given):
                for p in parts:
                    res.append(f.contains(p))

            return or_(*res)
        else:
            return op(cls.formatted._val(cls, q) == value)

    @formatted.value
    def formatted_value(cls, q: Query):
        return func.CONCAT_WS(" ", cls.title, cls.family, cls.given)

    @formatted.order
    def formatted_order(cls, q: Query, op):
        return op(func.CONCAT_WS(" ", cls.family, cls.given))


class Address(Entity):
    id: Serial
    title: String


class User(Entity):
    id: Serial
    name: String
    email: String
    created_time: DateTimeTz

    address_id: Int = ForeignKey(Address.id)
    address: One[Address]
    tags: ManyAcross["UserTags", "Tag"]
    # birth_date:

    @virtual(depends="name")
    def name_q(self):
        return self.name

    @name_q.compare
    def name_q_compare(cls, q: Query, op: Any, value: Any):
        if op is contains:
            res = []
            parts = value.split()

            for p in parts:
                res.append(cls.name.contains(p))

            return or_(*res)
        else:
            return op(cls.name, value)

    @classmethod
    def query_by_email(cls, email: str) -> Query:
        return Query(cls).where(cls.email == email)


class Tag(Entity):
    id: Serial
    value: String = Field(size=50)


class UserTags(Entity):
    user_id: Serial = ForeignKey(User.id)
    tag_id: Serial = ForeignKey(Tag.id)


def test_query_basics():
    q = Query()
    q.select_from(User)
    q.where(User.id == 42)

    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."id" = $1'
    assert params == (42, )

    q = Query(User).columns(func.min(User.created_time))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT min("t0"."created_time") FROM "User" "t0"'

    q = User.query_by_email("email@example.com")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."email" = $1'
    assert params == ("email@example.com", )


def test_query_and_or():
    q = Query()
    q.select_from(User)
    q.where(or_(User.id == 1, User.id == 2, User.id == 3))
    q.where(User.email == "email")
    q.where(or_(and_(User.id == 1, User.id == 2), User.id == 3))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ("t0"."id" = $1 OR "t0"."id" = $2 OR "t0"."id" = $3) AND "t0"."email" = $4 AND (("t0"."id" = $1 AND "t0"."id" = $2) OR "t0"."id" = $3)'
    assert params == (1, 2, 3, "email")


def test_in():
    q = Query()
    q.select_from(User)
    q.where(User.id.in_(1, 2, 3))
    q.where(~User.id.in_(1, 2, 3))
    q.where(User.id.in_([1, 2, 3]))
    q.where(User.id.in_([1, User.email, 3]))
    q.where(User.id.in_(1, User.email, 3))

    sql, params = dialect.create_query_compiler().compile_select(q)

    # assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."id" IN ($1, $2, $3) AND "t0"."id" IN ($1, $2, $3) AND "t0"."id" IN ($1, "t0"."email", $3) AND "t0"."id" IN ($1, "t0"."email", $3)'
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."id" IN ($1, $2, $3) AND "t0"."id" NOT IN ($1, $2, $3) AND "t0"."id" IN ($1, $2, $3) AND "t0"."id" IN ($1, "t0"."email", $3) AND "t0"."id" IN ($1, "t0"."email", $3)"""
    assert params == (1, 2, 3)


def test_in_eq():
    q = Query()
    q.select_from(User)
    q.where(User.id.in_(1, 2, 3) == True)

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."id" IN ($1, $2, $3) IS TRUE'
    assert params == (1, 2, 3)


def test_is_true():
    q = Query()
    q.select_from(User)
    q.where(User.id.is_true())
    q.where(~User.id.is_true())

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."id" IS TRUE AND "t0"."id" IS NOT TRUE'
    assert params == ()


def test_group_by():
    q = Query()
    q.select_from(User) \
        .group(User.id) \
        .group(User.name, User.email) \
        .group(User.name == 42)

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" GROUP BY "t0"."id", "t0"."name", "t0"."email", "t0"."name" = $1'
    assert params == (42, )


def test_order_by():
    q = Query()
    q.select_from(User) \
        .order(User.id.asc()) \
        .order(User.name) \
        .order(User.email.desc()) \
        .order(User.email == "email")

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" ORDER BY "t0"."id" ASC, "t0"."name" ASC, "t0"."email" DESC, ("t0"."email" = $1) ASC'
    assert params == ("email", )


def test_having():
    q = Query()
    q.select_from(User) \
        .having(User.id == 42) \
        .having(~User.name.is_null()) \

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" HAVING "t0"."id" = $1 AND "t0"."name" IS NOT NULL'
    assert params == (42, )


def test_limit_offset():
    q = Query()
    q.select_from(User).limit(20).offset(0)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FETCH FIRST 20 ROWS ONLY'

    q = Query()
    q.select_from(User).offset(0).limit(20)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FETCH FIRST 20 ROWS ONLY'

    q = Query()
    q.select_from(User).limit(20).offset(1)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" OFFSET 1 FETCH FIRST 20 ROWS ONLY'

    q = Query()
    q.select_from(User).offset(1).limit(20)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" OFFSET 1 FETCH FIRST 20 ROWS ONLY'

    q = Query()
    q.select_from(User).limit(20)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FETCH FIRST 20 ROWS ONLY'

    q = Query()
    q.select_from(User).limit(1)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FETCH FIRST ROW ONLY'

    q = Query()
    q.select_from(User).offset(20)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" OFFSET 20'


def test_field_alias():
    q = Query()
    q.select_from(User).columns(User.id, User.id.alias("id2"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."id" as "id2" FROM "User" "t0"'


def test_query_alias():
    sq = Query().select_from(User).columns(User.email).where(User.id == 42)

    q = Query()
    q.select_from(User).columns(User.id, User.id.alias("id2"), sq.alias("xyz_email")).where(User.id == 24)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."id" as "id2", (SELECT "t1"."email" FROM "User" "t1" WHERE "t1"."id" = $1) as "xyz_email" FROM "User" "t0" WHERE "t0"."id" = $2'
    assert params == (42, 24)


def test_entity_alias():
    TEST = User.alias("TEST")

    q = Query().select_from(TEST).where(TEST.id == 12)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "TEST"."id", "TEST"."name", "TEST"."email", "TEST"."created_time", "TEST"."address_id" FROM "User" "TEST" WHERE "TEST"."id" = $1'
    assert params == (12, )

    q2 = Query().select_from(User).where(q > 0)
    sql, params = dialect.create_query_compiler().compile_select(q2)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE (SELECT "TEST"."id", "TEST"."name", "TEST"."email", "TEST"."created_time", "TEST"."address_id" FROM "User" "TEST" WHERE "TEST"."id" = $1) > $2'
    assert params == (12, 0)

    q = Query().select_from(TEST).join(TEST.address) \
        .where(TEST.address.title == "OK") \
        .where(TEST.id == 42)

    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "TEST"."id", "TEST"."name", "TEST"."email", "TEST"."created_time", "TEST"."address_id" FROM "User" "TEST" INNER JOIN "Address" "t0" ON "TEST"."address_id" = "t0"."id" WHERE "t0"."title" = $1 AND "TEST"."id" = $2'
    assert params == ("OK", 42)

    q = Query().select_from(TEST).join(TEST.tags).where(TEST.tags.value == 42)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "TEST"."id", "TEST"."name", "TEST"."email", "TEST"."created_time", "TEST"."address_id" FROM "User" "TEST" INNER JOIN "UserTags" "t0" ON "t0"."user_id" = "TEST"."id" INNER JOIN "Tag" "t1" ON "t0"."tag_id" = "t1"."id" WHERE "t1"."value" = $1'
    assert params == (42, )

    q = TEST.query_by_email("email@example.com")
    sql, params = dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == 'SELECT "TEST"."id", "TEST"."name", "TEST"."email", "TEST"."created_time", "TEST"."address_id" FROM "User" "TEST" WHERE "TEST"."email" = $1'
    assert params == ("email@example.com", )


def test_entity_alias_mixin():
    R = Registry()

    class Mixin:
        created_time: DateTimeTz

    class A(Entity, Mixin, registry=R):
        id: Serial

    q = Query(A).order(A.created_time)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."created_time" FROM "A" "t0" ORDER BY "t0"."created_time" ASC"""

    alias = A.alias("XXX")
    q = Query(alias).order(alias.created_time)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "XXX"."id", "XXX"."created_time" FROM "A" "XXX" ORDER BY "XXX"."created_time" ASC"""


def test_join_relation():
    q = Query().select_from(User).join(User.address)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" INNER JOIN "Address" "t1" ON "t0"."address_id" = "t1"."id"'


def test_join_across_relation():
    q = Query().select_from(User).join(User.tags).where(User.tags.value == "nice")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" INNER JOIN "UserTags" "t1" ON "t1"."user_id" = "t0"."id" INNER JOIN "Tag" "t2" ON "t1"."tag_id" = "t2"."id" WHERE "t2"."value" = $1'
    assert params == ("nice", )


def test_join_entity():
    q = Query().select_from(User).join(Address).where(Address.title == "address")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" INNER JOIN "Address" "t1" ON "t0"."address_id" = "t1"."id" WHERE "t1"."title" = $1'
    assert params == ("address", )


def test_join_entity_across():
    q = Query().select_from(User).join(UserTags).join(Tag).where(Tag.value == "address")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" INNER JOIN "UserTags" "t1" ON "t1"."user_id" = "t0"."id" INNER JOIN "Tag" "t2" ON "t1"."tag_id" = "t2"."id" WHERE "t2"."value" = $1'
    assert params == ("address", )


def test_auto_join_relation():
    q = Query().select_from(User).where(User.tags.value == "nice")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" INNER JOIN "UserTags" "t1" ON "t1"."user_id" = "t0"."id" INNER JOIN "Tag" "t2" ON "t1"."tag_id" = "t2"."id" WHERE "t2"."value" = $1'
    assert params == ("nice", )


@pytest.mark.skip(reason="deferred")
def test_eager_load():
    q = Query().select_from(User).columns(User, User.address, User.tags)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == ""
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
    (startswith, User.email, 1, '"t0"."email" ILIKE ($1 || \'%\')'),
    (endswith, User.email, 1, '"t0"."email" ILIKE (\'%\' || $1)'),
    (contains, User.email, 1, '"t0"."email" ILIKE (\'%\' || $1 || \'%\')'),
    (find, User.email, 1, 'POSITION(LOWER($1) IN LOWER("t0"."email"))'),
]


@pytest.mark.parametrize("op,left,right,expected", binary_operator_cases, ids=[x[3] for x in binary_operator_cases])
def test_binary_operators(op, left, right, expected):
    q = Query()
    q.select_from(User)
    q.where(op(left, right))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ' + expected


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

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ' + expected


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

    assert sql == f'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."id" {original} "t0"."email" AND "t0"."id" {inverted} "t0"."email"'


def test_op_precedence():
    q = Query().select_from(User).where((User.id - User.id) / (2000 / User.id))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ("t0"."id" - "t0"."id") / ($1 / "t0"."id")"""
    assert params == (2000,)

    q = Query().select_from(User).where((User.id - User.id) / (User.id - 2000 - User.email - (User.id * (3 - User.id))))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ("t0"."id" - "t0"."id") / ((("t0"."id" - $1) - "t0"."email") - ("t0"."id" * ($2 - "t0"."id")))"""
    assert params == (2000, 3)


def test_call():
    q = Query()
    q.select_from(User)
    q.where(func.DATE_FORMAT(User.created_time, "%Y-%m-%d") == "2019-01-01")

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE DATE_FORMAT("t0"."created_time", $1) = $2'
    assert params == ("%Y-%m-%d", "2019-01-01")


class UserJson(Entity):
    id: Serial
    name: Json[FullName]


class Article(Entity):
    id: Serial
    author_id: Int = ForeignKey(UserJson.id)
    author: One[UserJson]


def test_json():
    q = Query().select_from(UserJson).where(UserJson.name.family == "Kiss")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name" FROM "UserJson" "t0" WHERE jsonb_extract_path("t0"."name", 'family') = $1"""
    assert params == ("Kiss", )

    q = Query().select_from(Article) \
        .where(Article.author.name.family == "Kiss") \
        .where(Article.author.name.xyz.z == 1)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."author_id" FROM "Article" "t0" INNER JOIN "UserJson" "t1" ON "t0"."author_id" = "t1"."id" WHERE jsonb_extract_path("t1"."name", 'family') = $1 AND jsonb_extract_path("t1"."name", 'xyz', 'z') = $2"""
    assert params == ("Kiss", 1)


class UserComp2(Entity):
    id: Serial
    name: Composite[FullName]


class Article2(Entity):
    id: Serial
    author_id: Int = ForeignKey(UserComp2.id)
    author: One[UserComp2]


def test_composite():

    q = Query().select_from(UserComp2).where(UserComp2.name.family == "Teszt")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", ("t0"."name")."title", ("t0"."name")."family", ("t0"."name")."given", ("t0"."name")."xyz" FROM "UserComp2" "t0" WHERE ("t0"."name")."family" = $1"""
    assert params == ("Teszt", )

    q = Query().select_from(Article2) \
        .where(Article2.author.name.family == "Teszt") \
        .where(Article2.author.name.xyz.z == 1)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."author_id" FROM "Article2" "t0" INNER JOIN "UserComp2" "t1" ON "t0"."author_id" = "t1"."id" WHERE ("t1"."name")."family" = $1 AND jsonb_extract_path(("t1"."name")."xyz", 'z') = $2"""
    assert params == ("Teszt", 1)


reg_ambiguous = Registry()


class UserA(Entity, registry=reg_ambiguous):
    id: Serial


class ArticleA(Entity, registry=reg_ambiguous):
    id: Serial

    creator_id: Auto = ForeignKey(UserA.id)
    creator: One[UserA] = "UserA.id == ArticleA.creator_id"

    updater_id: Auto = ForeignKey(UserA.id)
    updater: One[UserA] = "UserA.id == ArticleA.updater_id"


def test_ambiguous():
    q = Query().select_from(ArticleA) \
        .where(ArticleA.creator.id == 1) \
        .where(ArticleA.updater.id.is_null())
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."creator_id", "t0"."updater_id" FROM "ArticleA" "t0" INNER JOIN "UserA" "t1" ON "t1"."id" = "t0"."creator_id" INNER JOIN "UserA" "t2" ON "t2"."id" = "t0"."updater_id" WHERE "t1"."id" = $1 AND "t2"."id" IS NULL"""
    assert params == (1, )


def test_virtual():
    q = Query(User).where(User.name_q.contains("Jane Doe"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."name" ILIKE ('%' || $1 || '%') OR "t0"."name" ILIKE ('%' || $2 || '%')"""
    assert params == ("Jane", "Doe")

    q = Query(User).where(or_(User.name_q.contains("Jane Doe"), User.name_q.contains("Jhon Smith")))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."name" ILIKE ('%' || $1 || '%') OR "t0"."name" ILIKE ('%' || $2 || '%') OR "t0"."name" ILIKE ('%' || $3 || '%') OR "t0"."name" ILIKE ('%' || $4 || '%')"""
    assert params == ("Jane", "Doe", "Jhon", "Smith")

    UserA = User.alias("UserA")
    q = Query(UserA).where(UserA.name_q.contains("Jane Doe"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "UserA"."id", "UserA"."name", "UserA"."email", "UserA"."created_time", "UserA"."address_id" FROM "User" "UserA" WHERE "UserA"."name" ILIKE ('%' || $1 || '%') OR "UserA"."name" ILIKE ('%' || $2 || '%')"""
    assert params == ("Jane", "Doe")

    q = Query(User).load(User.name_q)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."name" FROM "User" "t0"'

    q = Query(UserA).load(UserA.name_q)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "UserA"."name" FROM "User" "UserA"'


class UserComp3(Entity):
    id: Serial
    name: Composite[FullName]


def test_virtual_composite():
    q = Query(UserComp3).where(UserComp3.name.formatted.contains("Jane Doe"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", ("t0"."name")."title", ("t0"."name")."family", ("t0"."name")."given", ("t0"."name")."xyz" FROM "UserComp3" "t0" WHERE ("t0"."name")."family" ILIKE ('%' || $1 || '%') OR ("t0"."name")."family" ILIKE ('%' || $2 || '%') OR ("t0"."name")."given" ILIKE ('%' || $1 || '%') OR ("t0"."name")."given" ILIKE ('%' || $2 || '%')"""
    assert params == ("Jane", "Doe")


class UserCompVR(Entity):
    id: Serial
    name: Composite[FullName]


class ArticleVR(Entity):
    id: Serial
    user_id: Auto = ForeignKey(UserCompVR.id)
    user: One[UserCompVR]

    @virtual(depends=("user.name", ))
    def user_name(self):
        return self.user.name.formatted


def test_virtual_relation():
    q = Query(ArticleVR) \
        .columns(ArticleVR.user.name.formatted) \
        .where(ArticleVR.user.name.formatted.contains("Jane Doe")) \
        .order(ArticleVR.user.name.formatted.desc())
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT CONCAT_WS(' ', ("t1"."name")."title", ("t1"."name")."family", ("t1"."name")."given") FROM "ArticleVR" "t0" INNER JOIN "UserCompVR" "t1" ON "t0"."user_id" = "t1"."id" WHERE ("t1"."name")."family" ILIKE ('%' || $1 || '%') OR ("t1"."name")."family" ILIKE ('%' || $2 || '%') OR ("t1"."name")."given" ILIKE ('%' || $1 || '%') OR ("t1"."name")."given" ILIKE ('%' || $2 || '%') ORDER BY CONCAT_WS(' ', ("t1"."name")."family", ("t1"."name")."given") DESC"""
    assert params == ("Jane", "Doe")

    q = Query(ArticleVR).load(ArticleVR.user_name)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT (SELECT ROW(("t2"."name")."title", ("t2"."name")."family", ("t2"."name")."given", ("t2"."name")."xyz") FROM "UserCompVR" "t2" WHERE "t1"."user_id" = "t2"."id") as "t0" FROM "ArticleVR" "t1"'


class AlwaysLoad1(Entity):
    id: Int
    user_id: Auto = ForeignKey(UserCompVR.id)
    user: One[UserCompVR] = Loading(always=True)


class AlwaysLoad2(Entity):
    id: Int
    user_id: Auto = ForeignKey(UserCompVR.id)
    user: One[UserCompVR] = Loading(always=True, fields=["name"])


class AlwaysLoad3(Entity):
    id: Int
    article_id: Auto = ForeignKey(ArticleVR.id)
    article: One[ArticleVR] = Loading(always=True, fields=["user.name"])


class AlwaysLoad4(Entity):
    id: Int
    article_id: Auto = ForeignKey(ArticleVR.id)
    article: One[ArticleVR] = Loading(always=True, fields=["user_name"])


def test_always_load():
    q = Query(AlwaysLoad1)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t1"."id", "t1"."user_id", (SELECT ROW("t2"."id", ("t2"."name")."title", ("t2"."name")."family", ("t2"."name")."given", ("t2"."name")."xyz") FROM "UserCompVR" "t2" WHERE "t1"."user_id" = "t2"."id") as "t0" FROM "AlwaysLoad1" "t1"'

    q = Query(AlwaysLoad2)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t1"."id", "t1"."user_id", (SELECT ROW(("t2"."name")."title", ("t2"."name")."family", ("t2"."name")."given", ("t2"."name")."xyz") FROM "UserCompVR" "t2" WHERE "t1"."user_id" = "t2"."id") as "t0" FROM "AlwaysLoad2" "t1"'

    q = Query(AlwaysLoad3)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t2"."id", "t2"."article_id", (SELECT ROW((SELECT ROW(("t4"."name")."title", ("t4"."name")."family", ("t4"."name")."given", ("t4"."name")."xyz") FROM "UserCompVR" "t4" WHERE "t3"."user_id" = "t4"."id")) FROM "ArticleVR" "t3" WHERE "t2"."article_id" = "t3"."id") as "t0" FROM "AlwaysLoad3" "t2"'

    q = Query(AlwaysLoad4)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t2"."id", "t2"."article_id", (SELECT ROW((SELECT ROW(("t4"."name")."title", ("t4"."name")."family", ("t4"."name")."given", ("t4"."name")."xyz") FROM "UserCompVR" "t4" WHERE "t3"."user_id" = "t4"."id")) FROM "ArticleVR" "t3" WHERE "t2"."article_id" = "t3"."id") as "t0" FROM "AlwaysLoad4" "t2"'


class Deep(Entity):
    id: Serial
    user_id: Auto = ForeignKey(User.id)
    user: One[User]


def test_deep_raltion():
    q = Query(Deep).where(Deep.user.tags.value == "OK")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."user_id" FROM "Deep" "t0" INNER JOIN "User" "t1" ON "t0"."user_id" = "t1"."id" INNER JOIN "UserTags" "t2" ON "t2"."user_id" = "t1"."id" INNER JOIN "Tag" "t3" ON "t2"."tag_id" = "t3"."id" WHERE "t3"."value" = $1"""
    assert params == ("OK", )


def test_join_type_in_or():
    R = Registry()

    class A(Entity, registry=R):
        id: Serial

    class B(Entity, registry=R):
        id: Serial
        a_id: Auto = ForeignKey(A.id)
        a: One[A]

    q = Query(B).where(B.a.id == 2)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."a_id" FROM "B" "t0" INNER JOIN "A" "t1" ON "t0"."a_id" = "t1"."id" WHERE "t1"."id" = $1"""
    assert params == (2, )

    q = Query(B).where(or_(B.a.id == 2, B.id == 3))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."a_id" FROM "B" "t0" LEFT JOIN "A" "t1" ON "t0"."a_id" = "t1"."id" WHERE "t1"."id" = $1 OR "t0"."id" = $2"""
    assert params == (2, 3)





def test_over():
    R = Registry()

    class A(Entity, registry=R):
        id: Serial

    class B(Entity, registry=R):
        id: Serial
        a_id: Auto = ForeignKey(A.id)
        a: One[A]

    q = Query(A).columns(func.row_number().over().order(A.id.desc()))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT row_number() OVER(ORDER BY "t0"."id" DESC) FROM "A" "t0"'
    assert len(params) == 0

    q = Query(A).columns(func.row_number().over().partition(A.id))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT row_number() OVER(PARTITION BY "t0"."id") FROM "A" "t0"'
    assert len(params) == 0

    q = Query(B).columns(func.row_number().over().order(B.a.id.desc()))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT row_number() OVER(ORDER BY "t1"."id" DESC) FROM "B" "t0" INNER JOIN "A" "t1" ON "t0"."a_id" = "t1"."id"'
    assert len(params) == 0

    q = Query(B).columns(func.row_number().over().partition(B.a.id))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT row_number() OVER(PARTITION BY "t1"."id") FROM "B" "t0" INNER JOIN "A" "t1" ON "t0"."a_id" = "t1"."id"'
    assert len(params) == 0

    q = Query(A).columns(func.row_number().over().partition(A.id).order(A.id))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT row_number() OVER(PARTITION BY "t0"."id" ORDER BY "t0"."id" ASC) FROM "A" "t0"'
    assert len(params) == 0

def test_enum():
    R = Registry()

    class Status(Enum, registry=R):
        retryable: Bool = False

        PENDING = dict(label="Pending", retryable=True)
        SUCCESS = dict(label="Success", retryable=False)
        FAILURE = dict(label="Failure", retryable=False)

    class X(Entity, registry=R):
        id: Serial
        value: Int

    class A(Entity, registry=R):
        id: Serial
        status: Choice[Status]
        x: One[X]

    class B(Entity, registry=R):
        id: Serial
        a: One[A] = "A.id == B.id"

    q = Query(A)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."status" FROM "A" "t0"'
    assert len(params) == 0

    q = Query(A).where(A.status.retryable.is_true())
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."status" FROM "A" "t0" INNER JOIN "Status" "t1" ON "t0"."status" = "t1"."value" WHERE "t1"."retryable" IS TRUE'
    assert len(params) == 0

    a2 = A.alias()
    q = Query(A).join(a2, a2.id == A.id).where(A.status.retryable.is_true(), a2.status.retryable.is_false())
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."status" FROM "A" "t0" INNER JOIN "A" "t1" ON "t1"."id" = "t0"."id" INNER JOIN "Status" "t2" ON "t0"."status" = "t2"."value" INNER JOIN "Status" "t3" ON "t1"."status" = "t3"."value" WHERE "t2"."retryable" IS TRUE AND "t3"."retryable" IS FALSE'
    assert len(params) == 0

    q = Query(A).where(A.status == "PENDING")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."status" FROM "A" "t0" WHERE "t0"."status" = $1'
    assert params == ("PENDING",)

    q = Query(B).where(B.a.status.retryable.is_true())
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id" FROM "B" "t0" INNER JOIN "A" "t1" ON "t1"."id" = "t0"."id" INNER JOIN "Status" "t2" ON "t1"."status" = "t2"."value" WHERE "t2"."retryable" IS TRUE'
    assert len(params) == 0


def test_raw():
    q = Query(User).where(raw("FULL INVALID"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE FULL INVALID'''
    assert params == ()

    q = Query(User).where(raw("ARRAY[", User.id, "]", " && ", "ARRAY[", param(1), ',', param(2), "]"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ARRAY["t0"."id"] && ARRAY[$1,$2]'''
    assert params == (1, 2)

    q = Query(User).where(raw("ARRAY[", param("ALMA"), ',', User.name, "]"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ARRAY[$1,"t0"."name"]'''
    assert params == ("ALMA",)

    q = Query(User).where(raw("ARRAY[", param("ALMA"), ',', User.name, "]")).where(User.id == 42)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE ARRAY[$1,"t0"."name"] AND "t0"."id" = $2'''
    assert params == ("ALMA", 42)


def test_order_by_binary_expr():
    q = Query(User).order(and_(User.id == 1, User.name == "Alma"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" ORDER BY ("t0"."id" = $1 AND "t0"."name" = $2) ASC'''
    assert params == (1, "Alma")


def test_lock():
    q = Query(User).for_update()
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR UPDATE"""

    q = Query(User).for_no_key_update()
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR NO KEY UPDATE"""

    q = Query(User).for_share()
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR SHARE"""

    q = Query(User).for_key_share()
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR KEY SHARE"""

    q = Query(User).for_update(User)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR UPDATE OF "t0"'''

    q = Query(User).for_update(User, nowait=True)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR UPDATE OF "t0" NOWAIT'''

    q = Query(User).for_update(User, skip=True)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR UPDATE OF "t0" SKIP LOCKED'''

    q = Query(User).for_update(nowait=True)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR UPDATE NOWAIT'''

    q = Query(User).for_update(skip=True)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" FOR UPDATE SKIP LOCKED'''

    # TODO
    # q = Query(User).load(User.id, User.tags).for_update(User.tags, skip=True)
    # sql, params = dialect.create_query_compiler().compile_select(q)
    # assert sql == ""


def test_query_columns_cast():
    q = Query(User).columns(User.id.cast("TEXT"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id"::TEXT FROM "User" "t0"'


def test_query_without_from():
    q1 = Query(User).columns(User.email).where(User.id == 42)
    q2 = Query(User).columns(User.email).where(User.id == 56)
    q = Query().columns(func.coalesce(q1, q2))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT coalesce((SELECT "t0"."email" FROM "User" "t0" WHERE "t0"."id" = $1), (SELECT "t1"."email" FROM "User" "t1" WHERE "t1"."id" = $2))'
    assert params == (42, 56)
