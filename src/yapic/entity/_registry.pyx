import cython
from enum import Enum
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

    cpdef keys(self):
        return self.entities.keys()

    cpdef values(self):
        return self.entities.values()

    cpdef items(self):
        return self.entities.items()


class RegistryDiffKind(Enum):
    REMOVED = 1
    CREATED = 2
    CHANGED = 3


@cython.final
cdef class RegistryDiff:
    def __cinit__(self, Registry a, Registry b, object entity_diff):
        self.a = a
        self.b = b
        self.changes = []

        a_names = set(a.keys())
        b_names = set(b.keys())

        for removed in sorted(a_names - b_names):
            self.changes.append((RegistryDiffKind.REMOVED, a[removed]))

        for created in sorted(b_names - a_names):
            self.changes.append((RegistryDiffKind.CREATED, b[created]))

        for maybe_changed in sorted(a_names & b_names):
            diff = entity_diff(a[maybe_changed], b[maybe_changed])
            if diff:
                self.changes.append((RegistryDiffKind.CHANGED, diff))

    def __bool__(self):
        return len(self.changes) > 0

    def __len__(self):
        return len(self.changes)

    def __iter__(self):
        return iter(self.changes)
