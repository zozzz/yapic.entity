import operator
import pytest

from yapic.entity.sql import PostgreDialect
from yapic.entity import (Query, Entity, Serial, String, DateTimeTz, Json, Composite, and_, or_, Int, ForeignKey, One,
                          ManyAcross, Field, func)

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
    q.where(User.id.in_([1, 2, 3]))
    q.where(User.id.in_([1, User.email, 3]))
    q.where(User.id.in_(1, User.email, 3))

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE "t0"."id" IN ($1, $2, $3) AND "t0"."id" IN ($1, $2, $3) AND "t0"."id" IN ($1, "t0"."email", $3) AND "t0"."id" IN ($1, "t0"."email", $3)'
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

    assert sql == 'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" ORDER BY "t0"."id" ASC, "t0"."name" ASC, "t0"."email" DESC, "t0"."email" = $1 ASC'
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
    q.select_from(User).column(User.id, User.id.alias("id2"))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."id" as "id2" FROM "User" "t0"'


def test_query_alias():
    sq = Query().select_from(User).column(User.email).where(User.id == 42)

    q = Query()
    q.select_from(User).column(User.id, User.id.alias("id2"), sq.alias("xyz_email")).where(User.id == 24)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "t0"."id", "t0"."id" as "id2", (SELECT "t0"."email" FROM "User" "t0" WHERE "t0"."id" = $1) as "xyz_email" FROM "User" "t0" WHERE "t0"."id" = $2'
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
    assert sql == 'SELECT "TEST"."id", "TEST"."name", "TEST"."email", "TEST"."created_time", "TEST"."address_id" FROM "User" "TEST" INNER JOIN "Address" "t1" ON "TEST"."address_id" = "t1"."id" WHERE "t1"."title" = $1 AND "TEST"."id" = $2'
    assert params == ("OK", 42)

    q = Query().select_from(TEST).join(TEST.tags).where(TEST.tags.value == 42)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == 'SELECT "TEST"."id", "TEST"."name", "TEST"."email", "TEST"."created_time", "TEST"."address_id" FROM "User" "TEST" INNER JOIN "UserTags" "t1" ON "t1"."user_id" = "TEST"."id" INNER JOIN "Tag" "t2" ON "t1"."tag_id" = "t2"."id" WHERE "t2"."value" = $1'
    assert params == (42, )


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
    q = Query().select_from(User).column(User, User.address, User.tags)
    sql, params = dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == ''
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


def test_call():
    q = Query()
    q.select_from(User)
    q.where(func.DATE_FORMAT(User.created_time, "%Y-%m-%d") == "2019-01-01")

    sql, params = dialect.create_query_compiler().compile_select(q)

    assert sql == f'SELECT "t0"."id", "t0"."name", "t0"."email", "t0"."created_time", "t0"."address_id" FROM "User" "t0" WHERE DATE_FORMAT("t0"."created_time", $1) = $2'
    assert params == ("%Y-%m-%d", "2019-01-01")


def test_json():
    class UserJson(Entity):
        id: Serial
        name: Json[FullName]

    class Article(Entity):
        id: Serial
        author_id: Int = ForeignKey(UserJson.id)
        author: One[UserJson]

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


def test_composite():
    class UserComp2(Entity):
        id: Serial
        name: Composite[FullName]

    class Article2(Entity):
        id: Serial
        author_id: Int = ForeignKey(UserComp2.id)
        author: One[UserComp2]

    q = Query().select_from(UserComp2).where(UserComp2.name.family == "Kiss")
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."name" FROM "UserComp2" "t0" WHERE "t0"."name"."family" = $1"""
    assert params == ("Kiss", )

    q = Query().select_from(Article2) \
        .where(Article2.author.name.family == "Kiss") \
        .where(Article2.author.name.xyz.z == 1)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."author_id" FROM "Article2" "t0" INNER JOIN "UserComp2" "t1" ON "t0"."author_id" = "t1"."id" WHERE "t1"."name"."family" = $1 AND jsonb_extract_path("t1"."name"."xyz", 'z') = $2"""
    assert params == ("Kiss", 1)
