from yapic.entity._entity import Entity as _Entity
from yapic.entity._registry cimport Registry


REGISTRY = Registry()


class Entity(_Entity, registry=REGISTRY, _root=True):
    @classmethod
    def __register__(cls):
        try:
            schema = cls.__meta__["schema"]
        except KeyError:
            name = cls.__name__
        else:
            name = f"{schema}.{cls.__name__}"

        (<Registry>cls.__registry__).register(name, cls)

