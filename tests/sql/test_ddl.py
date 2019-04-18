from enum import Enum, Flag

from yapic.entity import Entity, Int, Serial, String, Choice, Field, PrimaryKey, ForeignKey, Date, DateTime, DateTimeTz, Bool, func, const
from yapic.sql import PostgreDialect

dialect = PostgreDialect()
ddl = dialect.create_ddl_compiler()


def test_basic():
    class User(Entity):
        pass

    class User2(Entity, name="__user__"):
        pass

    class User3(Entity, schema="auth"):
        pass

    class User4(Entity, schema="auth", name="userX"):
        pass

    result = ddl.compile_entity(User)
    assert result == """CREATE TABLE "User" (
);"""

    result = ddl.compile_entity(User2)
    assert result == """CREATE TABLE "__user__" (
);"""

    result = ddl.compile_entity(User3)
    assert result == """CREATE TABLE "auth"."User3" (
);"""

    result = ddl.compile_entity(User4)
    assert result == """CREATE TABLE "auth"."userX" (
);"""


def test_int():
    class Ints(Entity):
        int_small: Int = Field(size=2)
        int_medium: Int = Field(size=4)
        int_large: Int = Field(size=8)

    result = ddl.compile_entity(Ints)
    assert result == """CREATE TABLE "Ints" (
  "int_small" INT2,
  "int_medium" INT4,
  "int_large" INT8
);"""

    class Pk_small(Entity):
        id: Int = Field(size=2) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=2)

    class Pk_medium(Entity):
        id: Int = Field(size=4) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=4)

    class Pk_large(Entity):
        id: Int = Field(size=8) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=8)

    result = ddl.compile_entity(Pk_small)
    assert result == """CREATE TABLE "Pk_small" (
  "id" SERIAL2 NOT NULL,
  "id2" SERIAL2 NOT NULL,
  PRIMARY KEY("id", "id2")
);"""

    result = ddl.compile_entity(Pk_medium)
    assert result == """CREATE TABLE "Pk_medium" (
  "id" SERIAL4 NOT NULL,
  "id2" SERIAL4 NOT NULL,
  PRIMARY KEY("id", "id2")
);"""

    result = ddl.compile_entity(Pk_large)
    assert result == """CREATE TABLE "Pk_large" (
  "id" SERIAL8 NOT NULL,
  "id2" SERIAL8 NOT NULL,
  PRIMARY KEY("id", "id2")
);"""


def test_string():
    class A(Entity):
        id: String = Field(size=[10, 10]) // PrimaryKey()
        name: String = Field(size=50)
        email: String = Field(size=50)
        description: String

    result = ddl.compile_entity(A)
    assert result == """CREATE TABLE "A" (
  "id" CHAR(10) NOT NULL,
  "name" VARCHAR(50),
  "email" VARCHAR(50),
  "description" TEXT,
  PRIMARY KEY("id")
);"""


def test_enum():
    class Mood(Enum):
        SAD = "sad"
        OK = "ok"
        HAPPY = "happy"

    class Color(Flag):
        RED = 1
        GREEN = 2
        BLUE = 4

    class A2(Entity):
        mood: Choice[Mood]
        colors: Choice[Color]

    result = ddl.compile_entity(A2)
    assert result == """CREATE TABLE "A2" (
  "mood" VARCHAR(5) CHECK("mood" IN ('sad', 'ok', 'happy')),
  "colors" BIT(3)
);"""


def test_fk():
    class A3(Entity):
        id: Serial

    class X(Entity, schema="x_schema", name="y"):
        id: Serial

    class Z(Entity):
        id1: Serial
        id2: Serial

    class B(Entity):
        id: Serial = ForeignKey(Z.id1, name="composite_fk")
        id_a: Int = ForeignKey(A3.id) // ForeignKey(Z.id2, name="composite_fk")
        id_x: Int = ForeignKey(X.id)

    result = ddl.compile_entity(Z)
    assert result == """CREATE TABLE "Z" (
  "id1" SERIAL4 NOT NULL,
  "id2" SERIAL4 NOT NULL,
  PRIMARY KEY("id1", "id2")
);"""

    result = ddl.compile_entity(B)
    assert result == """CREATE TABLE "B" (
  "id" SERIAL4 NOT NULL,
  "id_a" INT4,
  "id_x" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "composite_fk" FOREIGN KEY ("id", "id_a") REFERENCES "Z" ("id1", "id2") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_B__id_a-A3__id" FOREIGN KEY ("id_a") REFERENCES "A3" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_B__id_x-y__id" FOREIGN KEY ("id_x") REFERENCES "x_schema"."y" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);"""


def test_date():
    class A4(Entity):
        date: Date
        date_time: DateTime
        date_time_tz: DateTimeTz = func.now()
        date_time_tz2: DateTimeTz = const.CURRENT_TIMESTAMP

    result = ddl.compile_entity(A4)
    assert result == """CREATE TABLE "A4" (
  "date" DATE,
  "date_time" TIMESTAMP,
  "date_time_tz" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "date_time_tz2" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);"""


def test_bool():
    class A5(Entity):
        is_active: Bool = 1

    result = ddl.compile_entity(A5)
    assert result == """CREATE TABLE "A5" (
  "is_active" BOOLEAN NOT NULL DEFAULT TRUE
);"""
