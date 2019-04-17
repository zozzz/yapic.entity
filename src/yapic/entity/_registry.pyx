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
