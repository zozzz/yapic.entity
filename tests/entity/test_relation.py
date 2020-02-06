import pytest

from yapic.entity import (
    Entity,
    String,
    Int,
    Serial,
    One,
    Many,
    ManyAcross,
    Relation,
    Index,
    ForeignKey,
    Registry,
    Auto,
)
from yapic.entity._entity import NOTSET
from yapic.entity._relation import RelatedList


class GlobalA(Entity):
    id: Serial
    name: String
    b_list: ManyAcross["AcrossAB", "GlobalB"]


class GlobalB(Entity):
    id: Serial
    name: String
    a_list: ManyAcross["AcrossAB", "GlobalA"]


class AcrossAB(Entity):
    id_a: Serial = ForeignKey(GlobalA.id)
    id_b: Serial = ForeignKey(GlobalB.id)


def test_relation_one():
    class B(Entity):
        id_a: Int = ForeignKey(GlobalA.id)
        n_one: One[GlobalA]
        fw_one: One["GlobalA"]

    assert isinstance(B.n_one, Relation)
    assert B.n_one._impl_.joined is GlobalA
    assert isinstance(B.fw_one, Relation)
    assert B.fw_one._impl_.joined is GlobalA

    binst = B()
    assert binst.n_one is None
    binst.n_one = "Something wrong"
    assert binst.n_one == "Something wrong"

    assert binst.fw_one is None
    binst.fw_one = "VALUE"
    assert binst.fw_one == "VALUE"
    binst.fw_one = "VALUE2"
    assert binst.fw_one == "VALUE2"


def test_relation_many():
    class B(Entity):
        id_a: Int = ForeignKey(GlobalA.id)
        n_many: Many[GlobalA]
        fw_many: Many["GlobalA"]

    assert isinstance(B.n_many, Relation)
    assert B.n_many._impl_.joined is GlobalA
    assert isinstance(B.fw_many, Relation)
    assert B.fw_many._impl_.joined is GlobalA

    ginst = GlobalA(id=1)
    ginst2 = GlobalA(id=2)
    binst = B()
    assert isinstance(binst.n_many, list)

    binst.n_many.append(ginst)
    assert binst.n_many == [ginst]
    assert binst.__state__.changes(B.n_many) == ([ginst], [], [])

    binst.__state__.reset(B.n_many)
    assert binst.n_many == [ginst]
    assert binst.__state__.changes(B.n_many) is NOTSET

    binst.n_many = [ginst2]
    assert binst.n_many == [ginst2]
    assert binst.__state__.changes(B.n_many) == ([ginst2], [ginst], [])

    binst.__state__.reset()
    assert binst.n_many == [ginst2]
    assert binst.__state__.changes(B.n_many) is NOTSET

    g3 = GlobalA(id=3)
    g4 = GlobalA(id=4)

    binst.n_many = [g3, g4]
    assert binst.n_many == [g3, g4]
    assert binst.__state__.changes(B.n_many) == ([g3, g4], [ginst2], [])

    # binst.n_many = [10, 11, 12]
    # assert len(binst.n_many.__added__) == 3
    # assert len(binst.n_many.__removed__) == 2

    # assert binst.n_many.__added__ == [10, 11, 12]
    # assert binst.n_many.__removed__ == [4, 6]

    # binst.n_many.append(13)
    # binst.n_many.append(14)
    # binst.n_many.append(15)

    # assert binst.n_many.__added__ == [10, 11, 12, 13, 14, 15]
    # assert binst.n_many.__removed__ == [4, 6]

    # binst.n_many[2:5] = [100, 101, 102, 103]

    # assert list(binst.n_many) == [10, 11, 100, 101, 102, 103, 15]
    # assert binst.n_many.__added__ == [10, 11, 15, 100, 101, 102, 103]
    # assert binst.n_many.__removed__ == [4, 6, 12, 13, 14]

    # binst.fw_many = (1, 2, 3)
    # assert list(binst.fw_many) == [1, 2, 3]
    # del binst.fw_many
    # assert list(binst.fw_many) == []
    # assert binst.fw_many.__removed__ == [1, 2, 3]

    # b2 = B()
    # assert list(b2.n_many) == []
    # assert list(b2.n_many.__added__) == []
    # assert list(b2.n_many.__removed__) == []
    # assert list(b2.fw_many) == []
    # assert list(b2.fw_many.__added__) == []
    # assert list(b2.fw_many.__removed__) == []

    assert (repr(
        B.n_many._impl_.join_expr) == "<Expr <Field id_a: Int of B> <built-in function eq> <Field id: Int of GlobalA>>")


def test_relation_many_many():
    rel = GlobalA.b_list
    across_join_expr = rel._impl_.across_join_expr
    join_expr = rel._impl_.join_expr

    assert (repr(across_join_expr) ==
            "<Expr <Field id_a: Int of AcrossAB> <built-in function eq> <Field id: Int of GlobalA>>")
    assert (repr(join_expr) == "<Expr <Field id_b: Int of AcrossAB> <built-in function eq> <Field id: Int of GlobalB>>")
    assert rel._impl_.dependency == [GlobalB, GlobalA, AcrossAB]


def test_fk_field():
    class B(Entity):
        id: Int
        fk_1: Int = ForeignKey(GlobalA.id)
        fk_2: Int = ForeignKey("GlobalA.id")

    fk1 = B.fk_1._exts_[0]
    fk2 = B.fk_2._exts_[0]

    assert isinstance(fk1, ForeignKey)
    assert isinstance(fk2, ForeignKey)

    assert fk1.ref is GlobalA.id
    assert fk2.ref is GlobalA.id


def test_get_foreign_key_refs():
    registry = Registry()

    class A(Entity, registry=registry):
        id: Serial

    class B(Entity, registry=registry):
        id: Serial
        a_id: Auto = ForeignKey(A.id)

    class C(Entity, registry=registry):
        id: Serial
        a_id: Auto = ForeignKey(A.id)
        b_id: Auto = ForeignKey("B.id")

    class D(Entity, registry=registry):
        id: Serial
        a_id_1: Auto = ForeignKey(A.id)
        a_id_2: Auto = ForeignKey(A.id)

    result = registry.get_foreign_key_refs(A.id)
    assert result[0] == (B, ["a_id"])
    assert result[1] == (C, ["a_id"])
    assert result[2] == (D, ["a_id_1", "a_id_2"])

    result = registry.get_foreign_key_refs(B.id)
    assert result[0] == (C, ["b_id"])
