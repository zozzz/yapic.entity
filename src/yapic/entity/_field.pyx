import cython
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject

from ._expression cimport Expression, Visitor
from ._entity cimport EntityType, EntityBase
from ._factory cimport ForwardDecl, get_type_hints, new_instance, new_instance_from_forward, is_forward_decl


cdef class Field(Expression):
    def __cinit__(self, impl = None, *, name = None, default = None, size = None):
        self._impl = impl
        self._default = default
        self.name = name
        self.extensions = []

        if size is not None:
            if isinstance(size, list) or isinstance(size, tuple):
                if len(size) == 2:
                    self.min_size = size[0]
                    self.max_size = size[1]
                    if self.min_size > self.max_size:
                        raise ValueError("TODO:")
                else:
                    raise ValueError("TODO:")

            elif isinstance(size, int):
                self.min_size = 0
                self.max_size = size
            else:
                raise ValueError("TODO:")
        else:
            self.min_size = -1
            self.max_size = -1

    @property
    def default(self):
        return self._default

    @property
    def __impl__(self):
        if is_forward_decl(self._impl):
            self._impl = new_instance_from_forward(self._impl)
        return self._impl

    def __get__(self, instance, owner):
        if instance is None:
            return self
        elif isinstance(instance, EntityBase):
            return (<EntityBase>instance).__state__.get_value(self.index)
        else:
            raise TypeError("Instance must be 'None' or 'EntityBase'")

    def __set__(self, EntityBase instance, value):
        instance.__state__.set_value(self.index, value)

    def __delete__(self, EntityBase instance):
        instance.__state__.del_value(self.index)

    def __floordiv__(Field self, FieldExtension other):
        other.field = self
        self.extensions.append(other)
        return self

    # cdef bint init_from_type(self, object t):
    #     annots = get_annots(t)
    #     print(annots)


    cdef void bind(self, EntityType entity):
        self.entity = entity
        self.extensions = tuple(self.extensions)

        for ext in self.extensions:
            ext.bind(entity)

    cdef bint values_is_eq(self, object a, object b):
        # TODO: ...
        return a == b

    def __repr__(self):
        if self.entity:
            return "<Field %s %s of %s>" % (self.__impl__, self.name, self.entity)
        else:
            return "<Field %s %s (unbound)>" % (self.__impl__, self.name)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_field(self)


# cdef bint Field_init_attributes(object inst, dict attrs):
#     print("Field_init_attributes (%s, %s)", inst, attrs)
#     return True


cdef class FieldExtension:
    def __floordiv__(FieldExtension self, FieldExtension other):
        if not self.field:
            self.field = Field()
            self.field.extensions.append(self)

        other.field = self.field
        self.field.extensions.append(other)
        return self.field

    def bind(self, EntityType entity):
        pass


cdef class FieldImpl:
    cpdef object read(self, value):
        pass

    cpdef object write(self, value):
        pass

    cpdef bint eq(self, a, b):
        return self.write(a) == self.write(b)


cdef class StringImpl(FieldImpl):
    cpdef read(self, value):
        if isinstance(value, str):
            return value
        elif isinstance(value, bytes):
            return value.decode("utf-8")
        else:
            return str(value)

    cpdef write(self, value):
        if isinstance(value, bytes):
            return value
        elif not isinstance(value, str):
            value = str(value)
        return value.encode("utf-8")


cdef class IntImpl(FieldImpl):
    cpdef read(self, value):
        return int(value)

    cpdef write(self, value):
        return str(int(value)).encode("utf-8")

    cpdef bint eq(self, a, b):
        return a == b
