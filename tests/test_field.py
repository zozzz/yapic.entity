import pytest

from yapic.entity import field, Field
from yapic.entity._field import StringImpl


def test_field_default():
    assert Field(default="DEFAULT_VALUE").default == "DEFAULT_VALUE"
    assert Field().default is None


def test_field_size():
    f = Field()
    assert f.min_size == -1
    assert f.max_size == -1

    f = Field(size=1)
    assert f.min_size == 0
    assert f.max_size == 1

    f = Field(size=[10, 12])
    assert f.min_size == 10
    assert f.max_size == 12

    with pytest.raises(ValueError):
        Field(size=[13, 12])


def test_string_field():
    assert StringImpl in field.String.__args__
    assert str in field.String.__args__
    assert bytes in field.String.__args__

    sf = field.String()
    print(sf)
    bo = u"ő".encode("utf-8")
    uo = u"ő"

    assert sf.read(bo) == uo
    assert sf.read(uo) == uo
    assert sf.write(uo) == bo
    assert sf.write(bo) == bo
    assert sf.eq(bo, uo)
    assert sf.eq(uo, bo)
