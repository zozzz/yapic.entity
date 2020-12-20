from enum import Enum, Flag
import pytest

from yapic.entity import (Int, Serial, String, Choice, Field, PrimaryKey, ForeignKey, Date, DateTime, DateTimeTz, Bool,
                          func, const, Registry, Json, Composite, Auto, One, Index, StringArray, IntArray)
from yapic.entity.sql import PostgreDialect, Entity

dialect = PostgreDialect()
ddl = dialect.create_ddl_compiler()


class BaseEntity(Entity, registry=Registry(), _root=True):
    pass


def test_basic():
    class User(BaseEntity):
        pass

    class User2(BaseEntity, name="__user__"):
        pass

    class User3(BaseEntity, schema="auth"):
        pass

    class User4(BaseEntity, schema="auth", name="userX"):
        pass

    result = ddl.compile_entity(User)
    assert result[0] == """CREATE TABLE "User" (
);"""

    result = ddl.compile_entity(User2)
    assert result[0] == """CREATE TABLE "__user__" (
);"""

    result = ddl.compile_entity(User3)
    assert result[0] == """CREATE TABLE "auth"."User3" (
);"""

    result = ddl.compile_entity(User4)
    assert result[0] == """CREATE TABLE "auth"."userX" (
);"""


def test_int():
    class Ints(BaseEntity):
        int_small: Int = Field(size=2)
        int_medium: Int = Field(size=4)
        int_large: Int = Field(size=8)

    result = ddl.compile_entity(Ints)
    assert result[0] == """CREATE TABLE "Ints" (
  "int_small" INT2,
  "int_medium" INT4,
  "int_large" INT8
);"""

    class Pk_small(BaseEntity):
        id: Int = Field(size=2) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=2)

    class Pk_medium(BaseEntity):
        id: Int = Field(size=4) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=4)

    class Pk_large(BaseEntity):
        id: Int = Field(size=8) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=8)

    result = ddl.compile_entity(Pk_small)
    assert result[0] == """CREATE TABLE "Pk_small" (
  "id" INT2 NOT NULL,
  "id2" INT2 NOT NULL DEFAULT nextval('"Pk_small_id2_seq"'::regclass),
  PRIMARY KEY("id", "id2")
);"""

    result = ddl.compile_entity(Pk_medium)
    assert result[0] == """CREATE TABLE "Pk_medium" (
  "id" INT4 NOT NULL,
  "id2" INT4 NOT NULL DEFAULT nextval('"Pk_medium_id2_seq"'::regclass),
  PRIMARY KEY("id", "id2")
);"""

    result = ddl.compile_entity(Pk_large)
    assert result[0] == """CREATE TABLE "Pk_large" (
  "id" INT8 NOT NULL,
  "id2" INT8 NOT NULL DEFAULT nextval('"Pk_large_id2_seq"'::regclass),
  PRIMARY KEY("id", "id2")
);"""


def test_int_with_defaults():
    class IntWithDef(BaseEntity):
        int_small: Int = 0

    result = ddl.compile_entity(IntWithDef)
    assert result[0] == """CREATE TABLE "IntWithDef" (
  "int_small" INT4 NOT NULL DEFAULT 0
);"""


def test_string():
    class A(BaseEntity):
        id: String = Field(size=[10, 10]) // PrimaryKey()
        name: String = Field(size=50)
        email: String = Field(size=50)
        description: String

    result = ddl.compile_entity(A)
    assert result[0] == """CREATE TABLE "A" (
  "id" CHAR(10) NOT NULL,
  "name" VARCHAR(50),
  "email" VARCHAR(50),
  "description" TEXT,
  PRIMARY KEY("id")
);"""


@pytest.mark.skip(reason="Enum is in planning stage...")
def test_enum():
    class Mood(Enum):
        SAD = "sad"
        OK = "ok"
        HAPPY = "happy"

    class Color(Flag):
        RED = 1
        GREEN = 2
        BLUE = 4

    class A2(BaseEntity):
        mood: Choice[Mood]
        colors: Choice[Color]

    result = ddl.compile_entity(A2)
    assert result[0] == """CREATE TABLE "A2" (
  "mood" VARCHAR(5) CHECK("mood" IN ('sad', 'ok', 'happy')),
  "colors" BIT(3)
);"""


def test_index():
    class IndexedTable(BaseEntity):
        idx_1: Int = Index()
        idx_2: Int = Index(name="custom_name")
        idx_3: Int = Index(method="gin")
        idx_4: Int = Index(unique=True)
        idx_5: Int = Index(collate="hu_HU")

    result = ddl.compile_entity(IndexedTable)
    assert result[0] == """CREATE TABLE "IndexedTable" (
  "idx_1" INT4,
  "idx_2" INT4,
  "idx_3" INT4,
  "idx_4" INT4,
  "idx_5" INT4
);
CREATE INDEX "idx_IndexedTable__idx_1" ON "IndexedTable" USING btree ("idx_1");
CREATE INDEX "custom_name" ON "IndexedTable" USING btree ("idx_2");
CREATE INDEX "idx_IndexedTable__idx_3" ON "IndexedTable" USING gin ("idx_3");
CREATE UNIQUE INDEX "idx_IndexedTable__idx_4" ON "IndexedTable" USING btree ("idx_4");
CREATE INDEX "idx_IndexedTable__idx_5" ON "IndexedTable" USING btree ("idx_5") COLLATE "hu_HU";"""


def test_fk():
    class A3(BaseEntity):
        id: Serial

    class X(Entity, schema="x_schema", name="y"):
        id: Serial

    class Z(BaseEntity):
        id1: Serial
        id2: Serial

    class B(BaseEntity):
        id: Serial = ForeignKey(Z.id1, name="composite_fk")
        id_a: Int = ForeignKey(A3.id) // ForeignKey(Z.id2, name="composite_fk")
        id_x: Int = ForeignKey(X.id)

    result = ddl.compile_entity(Z)
    assert result[0] == """CREATE TABLE "Z" (
  "id1" INT4 NOT NULL DEFAULT nextval('"Z_id1_seq"'::regclass),
  "id2" INT4 NOT NULL DEFAULT nextval('"Z_id2_seq"'::regclass),
  PRIMARY KEY("id1", "id2")
);"""

    result = ddl.compile_entity(B)
    assert result[0] == """CREATE TABLE "B" (
  "id" INT4 NOT NULL DEFAULT nextval('"B_id_seq"'::regclass),
  "id_a" INT4,
  "id_x" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "composite_fk" FOREIGN KEY ("id", "id_a") REFERENCES "Z" ("id1", "id2") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_B__id_a-A3__id" FOREIGN KEY ("id_a") REFERENCES "A3" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_B__id_x-y__id" FOREIGN KEY ("id_x") REFERENCES "x_schema"."y" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX "idx_B__id_a" ON "B" USING btree ("id_a");
CREATE INDEX "idx_B__id_x" ON "B" USING btree ("id_x");"""


def test_date():
    class A4(BaseEntity):
        date: Date
        date_time: DateTime
        date_time_tz: DateTimeTz = func.now()
        date_time_tz2: DateTimeTz = const.CURRENT_TIMESTAMP

    result = ddl.compile_entity(A4)
    assert result[0] == """CREATE TABLE "A4" (
  "date" DATE,
  "date_time" TIMESTAMP,
  "date_time_tz" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "date_time_tz2" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);"""


def test_bool():
    class A5(BaseEntity):
        is_active: Bool = 1

    result = ddl.compile_entity(A5)
    assert result[0] == """CREATE TABLE "A5" (
  "is_active" BOOLEAN NOT NULL DEFAULT TRUE
);"""


def test_json():
    class Position(BaseEntity):
        x: Int
        y: Int

    class JsonTable(BaseEntity):
        id: Serial
        pos: Json[Position]

    result = ddl.compile_entity(JsonTable)
    assert result[0] == """CREATE TABLE "JsonTable" (
  "id" INT4 NOT NULL DEFAULT nextval('"JsonTable_id_seq"'::regclass),
  "pos" JSONB,
  PRIMARY KEY("id")
);"""


def test_composite():
    class FullName(BaseEntity):
        title: String
        family: String
        given: String

    class FNUser(BaseEntity):
        id: Serial
        name: Composite[FullName]

    result = ddl.compile_entity(FNUser)
    assert result[0] == """CREATE TABLE "FNUser" (
  "id" INT4 NOT NULL DEFAULT nextval('"FNUser_id_seq"'::regclass),
  "name" "FullName",
  PRIMARY KEY("id")
);"""


@pytest.mark.skip(reason="Implement recursion handling")
def test_self_ref():
    class Node(BaseEntity):
        id: Serial
        parent_id: Auto = ForeignKey("Node.id")
        parent: One["Node"]

    result = ddl.compile_entity(Node)
    assert result[0] == """CREATE TABLE "Node" (
  "id" INT4 NOT NULL DEFAULT nextval('"Node_id_seq"'::regclass),
  "parent_id" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_Node__parent_id-Node__id" FOREIGN KEY ("parent_id") REFERENCES "Node" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX "idx_Node__parent_id" ON "Node" USING btree ("parent_id");"""


def test_mixin():
    class FKUser(Entity):
        id: Serial

    class Mixin:
        created_time: DateTimeTz = func.now()
        user_id: Auto = ForeignKey(FKUser.id)

    class MEntity(BaseEntity, Mixin):
        id: Serial
        name: String = "Default Name"

    result = ddl.compile_entity(MEntity)
    assert result[0] == """CREATE TABLE "MEntity" (
  "id" INT4 NOT NULL DEFAULT nextval('"MEntity_id_seq"'::regclass),
  "name" TEXT NOT NULL DEFAULT 'Default Name',
  "created_time" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "user_id" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_MEntity__user_id-FKUser__id" FOREIGN KEY ("user_id") REFERENCES "FKUser" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE INDEX "idx_MEntity__user_id" ON "MEntity" USING btree ("user_id");"""


def test_array():
    class ArrayTest(Entity):
        id: Serial
        strings: StringArray
        ints: IntArray

    result = ddl.compile_entity(ArrayTest)
    assert result[0] == """CREATE TABLE "ArrayTest" (
  "id" INT4 NOT NULL DEFAULT nextval('"ArrayTest_id_seq"'::regclass),
  "strings" TEXT[],
  "ints" INT[],
  PRIMARY KEY("id")
);"""
