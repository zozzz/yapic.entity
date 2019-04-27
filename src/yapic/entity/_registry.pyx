import cython
from enum import Enum
from weakref import WeakValueDictionary

from ._entity cimport DependencyList
from ._entity_diff cimport EntityDiff


@cython.final
cdef class Registry:
    def __cinit__(self):
        # self.entities = WeakValueDictionary()
        self.entities = {}
        self.deferred = []

    cpdef object register(self, str name, EntityType entity):
        if name in self.entities:
            raise ValueError("entity already registered: %r" % entity)
        else:
            self.entities[name] = entity
            self.resolve_deferred()

            if not entity.resolve_deferred():
                self.deferred.append(entity)

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

    cdef resolve_deferred(self):
        cdef EntityType entity
        cdef list deferred = self.deferred
        cdef int index = len(deferred) - 1

        while index >= 0:
            entity = deferred[index]
            if entity.resolve_deferred():
                deferred.pop(index)

            index -= 1


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
        cdef DependencyList order = DependencyList()

        a_names = set(a.keys())
        b_names = set(b.keys())

        for removed in sorted(a_names - b_names):
            val = a[removed]
            self.changes.append((RegistryDiffKind.REMOVED, val))
            order.add(val)

        for created in sorted(b_names - a_names):
            val = b[created]
            self.changes.append((RegistryDiffKind.CREATED, val))
            order.add(val)

        for maybe_changed in sorted(a_names & b_names):
            val = b[maybe_changed]
            diff = entity_diff(a[maybe_changed], val)
            if diff:
                self.changes.append((RegistryDiffKind.CHANGED, diff))
                order.add(val)

        def key(item):
            kind, val = item
            if isinstance(val, EntityDiff):
                return order.index((<EntityDiff>val).b)
            else:
                return order.index(val)

        self.changes.sort(key=key)


    def __bool__(self):
        return len(self.changes) > 0

    def __len__(self):
        return len(self.changes)

    def __iter__(self):
        return iter(self.changes)
