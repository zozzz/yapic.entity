import cython
from weakref import WeakValueDictionary


@cython.final
cdef class Registry:
    def __cinit__(self):
        # self.entities = WeakValueDictionary()
        self.entities = {}

    cpdef object register(self, str name, EntityType entity):
        if name in self.entities:
            raise ValueError("entity already registered: %r" % entity)
        else:
            self.entities[name] = entity

    def __getitem__(self, str name):
        return self.entities[name]

    def __iter__(self):
        return iter(self.entities)


class RegistryDiffKind:
    REMOVED = 1
    CREATED = 2
    CHANGED = 3


@cython.final
cdef class RegistryDiff:
    def __cinit__(self, Registry a, Registry b, object entity_diff):
        self.changes = []
