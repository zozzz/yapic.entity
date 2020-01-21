import cython
from cpython.object cimport PyObject
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._expression cimport Expression, Visitor
from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeExt, EntityAttributeImpl, get_alias_target
from ._factory cimport ForwardDecl, get_type_hints, new_instance, new_instance_from_forward, is_forward_decl
from ._field_impl cimport AutoImpl


cdef class Field(EntityAttribute):
    def __cinit__(self, impl = None, *, name = None, default = None, size = None, nullable = None):
        self._default_ = default
        self._name_ = name
        self.type_cache = {}
        self.nullable = nullable

        if size is not None:
            if isinstance(size, list) or isinstance(size, tuple):
                if len(size) == 2:
                    self.min_size = size[0]
                    self.max_size = size[1]
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

    def __getattr__(self, key):
        return self._impl_.getattr(self, key)

    def __getitem__(self, key):
        return self._impl_.getitem(self, key)

    cdef object bind(self, EntityType entity):
        if self.nullable is None:
            self.nullable = True
            if self.get_ext(PrimaryKey):
                self.nullable = False
            elif not callable(self._default_):
                self.nullable = bool(self._default_ is None)
            elif isinstance(self._default_, Expression):
                self.nullable = False
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

    cpdef StorageType get_type(self, StorageTypeFactory factory):
        try:
            return self.type_cache[factory]
        except KeyError:
            type = factory.create(self)
            self.type_cache[factory] = type
            return type

    def __repr__(self):
        if self._entity_:
            return "<Field %s: %s of %s>" % (self._name_, self._impl_, self._entity_)
        else:
            return "<Field %s: %s (unbound)>" % (self._name_, self._impl_)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_field(self)

    cpdef copy_into(self, EntityAttribute other):
        EntityAttribute.copy_into(self, other)
        cdef Field other_field = other
        other_field.type_cache = self.type_cache

        if self.min_size >= 0:
            other_field.min_size = self.min_size

        if self.max_size >= 0:
            other_field.max_size = self.max_size

        other_field.nullable = self.nullable


# field_proxy_attrs = ("__proxied__", "__repr__", "clone")

# cdef class FieldProxy(Field):
#     def __cinit__(self, Field field):
#         self.__proxied__ = field

#     def __getattribute__(self, key):
#         if key in field_proxy_attrs:
#             return object.__getattribute__(self, key)
#         else:
#             return getattr(self.__proxied__, key)

#     def __getitem__(self, key):
#         return self.__proxied__[key]

#     def __repr__(self):
#         return "<FieldProxy %r>" % self.__proxied__

#     cpdef clone(self):
#         return type(self)(self.__proxied__.clone())


cdef class FieldExtension(EntityAttributeExt):
    pass


cdef class FieldImpl(EntityAttributeImpl):
    pass


cdef class StorageType:
    cpdef object encode(self, object value):
        raise NotImplementedError()

    cpdef object decode(self, object value):
        raise NotImplementedError()


cdef class StorageTypeFactory:
    cpdef StorageType create(self, Field field):
        raise NotImplementedError()


cdef class PrimaryKey(FieldExtension):
    pass


cdef class AutoIncrement(FieldExtension):
    def __cinit__(self, EntityType sequence=None):
        self.sequence = sequence

    cpdef object bind(self, EntityAttribute attr):
        cdef EntityType entity
        cdef EntityType aliased

        if self.sequence is None:
            entity = attr._entity_
            aliased = get_alias_target(entity)

            if entity is aliased:
                try:
                    schema = entity.__meta__["schema"]
                except KeyError:
                    schema = None

                name = f"{entity.__name__}_{attr._name_}_seq"
                self.sequence = EntityType(name, (EntityBase,), {}, schema=schema, registry=entity.__registry__, is_sequence=True)
            else:
                self.sequence = getattr(aliased, attr._key_).get_ext(AutoIncrement).sequence

        attr._deps_.add(self.sequence)
        return True

    def __repr__(self):
        return "@AutoIncrement(%r)" % self.sequence



cdef class Index(FieldExtension):
    def __cinit__(self, str expr = None, *, str name = None, str method = "btree", bint unique = False, bint concurrent = False, str collate = None):
        self.name = name
        self.method = method
        self.unique = unique
        self.concurrent = concurrent
        self.collate = collate
        self.expr = expr

    cpdef object clone(self):
        return type(self)(self.expr, name=self.name, method=self.method, unique=self.unique, concurrent=self.concurrent, collate=self.collate)

    cpdef object bind(self, EntityAttribute attr):
        if FieldExtension.bind(self, attr) is False:
            return False

        if not self.name:
            self.name = f"idx_{attr._name_}"

        return True

    def __repr__(self):
        return "@Index(%s USING %s ON %s)" % (self.name, self.method, self.attr)


cdef class Unique(FieldExtension):
    pass


# todo: faster eval, with Py_CompileString(ref, "<string>", Py_eval_input); PyEval_EvalCode

_CodeType = type(compile("1", "<string>", "eval"))

cdef class ForeignKey(FieldExtension):
    def __cinit__(self, field, *, str name = None, str on_update = "RESTRICT", str on_delete = "RESTRICT"):
        self.ref = None

        if isinstance(field, str):
            self._ref = compile(field, "<string>", "eval")
        elif isinstance(field, _CodeType):
            self._ref = field
        elif not isinstance(field, Field):
            raise TypeError("Incorrect argument for ForeignKey field parameter")
        else:
            self.ref = self._ref = field

        self.name = name
        self.on_update = on_update
        self.on_delete = on_delete

    cpdef object bind(self, EntityAttribute attr):
        if FieldExtension.bind(self, attr) is False:
            return False

        cdef Field field = attr

        if self.ref is None:
            module = PyImport_Import(self.attr._entity_.__module__)
            mdict = PyModule_GetDict(module)
            ldict = {attr._entity_.__qualname__.split(".").pop(): attr._entity_}
            try:
                self.ref = eval(self._ref, <object>mdict, <object>ldict)
            except NameError as e:
                return False

        attr._deps_.add(self.ref._entity_)

        if self.name is None:
            self.name = compute_fk_name(attr, self.ref)

        if isinstance(attr._impl_, AutoImpl):
            field._impl_ = self.ref._impl_
            field.min_size = self.ref.min_size
            field.max_size = self.ref.max_size

        cdef Index index = attr.get_ext(Index)
        if not index:
            index = Index()
            index.attr = attr
            attr._exts_.append(index)
            if index.bind(attr):
                index.bound = True
            else:
                return False

        return True

    cpdef object clone(self):
        return type(self)(self._ref, name=self.name, on_update=self.on_update, on_delete=self.on_delete)

    def __repr__(self):
        return "@ForeignKey(%s, %r, on_update=%s, on_delete=%s)" % (self.name, self.ref, self.on_update, self.on_delete)


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

    for k, v in fks.items():
        fks[k] = tuple(v)

    return fks
