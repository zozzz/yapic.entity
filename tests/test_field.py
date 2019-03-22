import pytest
from enum import Enum, Flag

from yapic.entity import field, Field, Entity
from yapic.entity._field_impl import StringImpl, ChoiceImpl


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
    bo = u"ő".encode("utf-8")
    uo = u"ő"

    # assert sf.read(bo) == uo
    # assert sf.read(uo) == uo
    # assert sf.write(uo) == bo
    # assert sf.write(bo) == bo
    # assert sf.eq(bo, uo)
    # assert sf.eq(uo, bo)


def test_choice_single_impl():
    class Mood(Enum):
        SAD = "sad"
        OK = "ok"
        HAPPY = "happy"

    choice_impl = ChoiceImpl(Mood)

    assert choice_impl.enum is Mood
    assert choice_impl.is_multi is False


def test_choice_multi_impl():
    class Color(Flag):
        RED = "red"
        GREEN = "green"
        BLUE = "blue"

    choice_impl = ChoiceImpl(Color)

    assert choice_impl.enum is Color
    assert choice_impl.is_multi is True


def test_choice_field_factory():
    class Mood(Enum):
        SAD = "sad"
        OK = "ok"
        HAPPY = "happy"

    class A(Entity):
        a: field.Choice[Mood]

    assert isinstance(A.a.__impl__, ChoiceImpl)
    assert A.a.__impl__.enum is Mood
    assert A.a.__impl__.is_multi is False
