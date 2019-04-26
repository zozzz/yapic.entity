import cython
from cpython.object cimport PyObject
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._expression cimport Expression, Visitor
from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeExt, EntityAttributeImpl
from ._factory cimport ForwardDecl, get_type_hints, new_instance, new_instance_from_forward, is_forward_decl


cdef class Field(EntityAttribute):
    def __cinit__(self, impl = None, *, name = None, default = None, size = None, nullable = None):
        self._default_ = default
        self._name_ = name
        self.type_cache = {}

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

    # def __get__(self, instance, owner):
    #     if instance is None:
    #         return self
    #     elif isinstance(instance, EntityBase):
    #         return (<EntityBase>instance).__fstate__.get_value(self._index_)
    #     else:
    #         raise TypeError("Instance must be 'None' or 'EntityBase'")

    # def __set__(self, EntityBase instance, value):
    #     instance.__fstate__.set_value(self._index_, value)

    # def __delete__(self, EntityBase instance):
    #     instance.__fstate__.del_value(self._index_)

    cdef object bind(self, EntityType entity):
        if self.nullable is None:
            if self.get_ext(PrimaryKey):
                self.nullable = False
            else:
                self.nullable = bool(self._default_ is None)
        return EntityAttribute.bind(self, entity)

    cpdef clone(self):
        cdef Field res = type(self)(self._impl_,
            name=self._name_,
            default=self._default_,
            size=(self.min_size, self.max_size),
            nullable=self.nullable)
        res._exts_ = self.clone_exts(res)
        res._deps_ = set(self._deps_)
        res.type_cache = self.type_cache
        return res

    # cdef bint values_is_eq(self, object a, object b):
    #     # TODO: ...
    #     return a == b

    cpdef StorageType get_type(self, StorageTypeFactory factory):
        try:
            return self.type_cache[factory]
        except KeyError:
            type = factory.create(self)
            self.type_cache[factory] = type
            return type

    def __repr__(self):
        if self._entity_:
            return "<Field %s: %s of %s>" % (self._name_, self._impl_, self._entity_.__name__)
        else:
            return "<Field %s: %s (unbound)>" % (self._name_, self._impl_)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_field(self)


# cdef bint Field_init_attributes(object inst, dict attrs):
#     print("Field_init_attributes (%s, %s)", inst, attrs)
#     return True


cdef class FieldExtension(EntityAttributeExt):
    pass


cdef class FieldImpl(EntityAttributeImpl):
    cpdef object init(self, EntityAttribute attr):
        return True


cdef class StorageType:
    cpdef object encode(self, object value):
        raise NotImplementedError()

    cpdef object decode(self, object value):
        raise NotImplementedError()


cdef class StorageTypeFactory:
    cpdef StorageType create(self, Field field):
        raise NotImplementedError()


cdef class PrimaryKey(FieldExtension):
    def __cinit__(self, *, bint auto_increment = False):
        self.auto_increment = auto_increment

    cpdef object clone(self):
        return type(self)(auto_increment=self.auto_increment)


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

        self.ref = None
        self.name = name
        self.on_update = on_update
        self.on_delete = on_delete

    cpdef object bind(self, EntityAttribute attr):
        if FieldExtension.bind(self, attr) is False:
            return False

        if self.ref is None:
            if not isinstance(self._ref, Field):
                module = PyImport_Import(self.attr._entity_.__module__)
                mdict = PyModule_GetDict(module)
                try:
                    self.ref = eval(self._ref, <object>mdict, None)
                except NameError as e:
                    return False
            else:
                self.ref = self._ref

        if self.name is None:
            self.name = compute_fk_name(attr, self.ref)
            attr._deps_.add(self.ref._entity_)

        return True

    cpdef object clone(self):
        return type(self)(self.ref, name=self.name, on_update=self.on_update, on_delete=self.on_delete)


cdef compute_fk_name(Field field_from, Field field_to):
    return "fk_%s__%s-%s__%s" % (
        field_from._entity_.__name__,
        field_from._name_,
        field_to._entity_.__name__,
        field_to._name_
    )


cdef dict collect_foreign_keys(EntityType entity):
    cdef dict fks = {}
    cdef Field field
    cdef ForeignKey fk
    cdef ForeignKey fkInFks

    for field in entity.__fields__:
        for ext in field._exts_:
            if isinstance(ext, ForeignKey):
                fk = <ForeignKey>ext
            else:
                continue

            if fk is not None:
                referenced = fk.ref

                if fk.name in fks:
                    for fkInFks in fks[fk.name]:
                        if fkInFks.ref._entity_ != referenced._entity_:
                            raise ValueError("Can't use fields from different entities in the same foreign key")
                        elif fk.on_update != fkInFks.on_update:
                            raise ValueError("Can't use different 'on_update' value in the same foreign key")
                        elif fk.on_delete != fkInFks.on_delete:
                            raise ValueError("Can't use different 'on_delete' value in the same foreign key")
                    fks[fk.name].append(fk)
                else:
                    fks[fk.name] = [fk]

    return fks
