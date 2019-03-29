import cython
from cpython.object cimport PyObject
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._expression cimport Expression, Visitor
from ._entity cimport EntityType, EntityBase
from ._factory cimport ForwardDecl, get_type_hints, new_instance, new_instance_from_forward, is_forward_decl


cdef class Field(Expression):
    def __cinit__(self, impl = None, *, name = None, default = None, size = None, nullable = None):
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
            return (<EntityBase>instance).__fstate__.get_value(self.index)
        else:
            raise TypeError("Instance must be 'None' or 'EntityBase'")

    def __set__(self, EntityBase instance, value):
        instance.__fstate__.set_value(self.index, value)

    def __delete__(self, EntityBase instance):
        instance.__fstate__.del_value(self.index)

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
            ext.init()

        if self.nullable is None:
            if self.get_ext(PrimaryKey):
                self.nullable = False
            else:
                self.nullable = bool(self._default is None)


    cdef bint values_is_eq(self, object a, object b):
        # TODO: ...
        return a == b

    cdef object get_ext(self, ext_type):
        for ext in self.extensions:
            if isinstance(ext, ext_type):
                return ext

    def __repr__(self):
        if self.entity:
            return "<Field %s: %s of %s>" % (self.name, self.__impl__, self.entity)
        else:
            return "<Field %s: %s (unbound)>" % (self.name, self.__impl__)

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

    def init(self):
        pass


cdef class PrimaryKey(FieldExtension):
    def __cinit__(self, *, bint auto_increment = False):
        self.auto_increment = auto_increment


cdef class Index(FieldExtension):
    pass


# todo: faster eval, with Py_CompileString(ref, "<string>", Py_eval_input); PyEval_EvalCode

cdef class ForeignKey(FieldExtension):
    def __cinit__(self, field, *, str name = None, str on_update = "RESTRICT", str on_delete = "RESTRICT"):
        # self._field = field
        if isinstance(field, str):
            self._ref = compile(field, "<string>", "eval")
        elif not isinstance(field, Field):
            raise TypeError("Incorrect argument for ForeignKey field parameter")
        else:
            self._ref = field

        self.name = name
        self.on_update = on_update
        self.on_delete = on_delete

    @property
    def ref(self):
        cdef PyObject* mdict

        if not isinstance(self._ref, Field):
            module = PyImport_Import(self.field.entity.__module__)
            mdict = PyModule_GetDict(module)
            self._ref = eval(self._ref, <object>mdict, None)

            if not isinstance(self._ref, Field):
                raise ValueError("Invalid value for ForeignKey field: %r" % self._ref)
            else:
                if self.name is None:
                    self.name = compute_fk_name(self.field, self._ref)

        return self._ref

    def init(self):
        super().init()
        if self.name is None:
            if isinstance(self._ref, Field):
                self.name = compute_fk_name(self.field, self._ref)


cdef compute_fk_name(Field field_from, Field field_to):
    return "fk_%s__%s-%s__%s" % (
        field_from.entity.__name__,
        field_from.name,
        field_to.entity.__name__,
        field_to.name
    )


cdef dict collect_foreign_keys(EntityType entity):
    cdef dict fks = {}
    cdef Field field
    cdef ForeignKey fk
    cdef ForeignKey fkInFks

    for field in entity.__fields__:
        fk = <ForeignKey>field.get_ext(ForeignKey)
        if fk is not None:
            referenced = fk.ref

            if fk.name in fks:
                for fkInFks in fks[fk.name]:
                    if fkInFks.ref.entity != referenced.entity:
                        raise ValueError("Can't use fields from different entities in the same foreign key")
                fks[fk.name].append(fk)
            else:
                fks[fk.name] = [fk]

    return fks
