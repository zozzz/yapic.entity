import hashlib
from ._entity cimport EntityType


cdef class Trigger:
    def __init__(self, *,
                 str name = None,
                 str before = None,
                 str after = None,
                 str for_each = None,
                 str when = None,
                 list params = None,
                 list args = None,
                 str body = None,
                 str unique_name = None):
        self.name = name
        self.before = before.upper() if before else None
        self.after = after.upper() if after else None
        self.for_each = for_each.upper() if for_each else None
        self.when = when
        self.params = params
        self.args = args
        self.body = body
        self.unique_name = unique_name

    cdef str get_unique_name(self, EntityType entity):
        if self.unique_name:
            return self.unique_name
        return f"YT-{entity.__name__}-{self.name}-{short_hash(self.when) + '-' if self.when else ''}{short_hash(self.body)}"

    def is_eq(self, EntityType entity, Trigger other):
        return self.before == other.before \
            and self.after == other.after \
            and self.for_each == other.for_each \
            and self.get_unique_name(entity) == other.get_unique_name(entity)

    # cdef clone(self):
    #     return type(self)(
    #         name=self.name,
    #         before=self.before,
    #         after=self.after,
    #         for_each=self.for_each,
    #         when=self.when,
    #         body=self.body,
    #     )

    # def __eq__(self, other):
    #     return isinstance(other, Trigger) \
    #         and self.unique_name == (<Trigger>other).unique_name

    # def __hash__(self):
    #     return hash(self.unique_name)


cdef class PolymorphParentDeleteTrigger(Trigger):
    def __init__(self, EntityType parent_entity):
        super().__init__(
            name=f"polyd_{parent_entity.__name__}",
            after="DELETE",
            for_each="ROW"
        )
        self.parent_entity = parent_entity

    cdef str get_unique_name(self, EntityType entity):
        if self.unique_name:
            return self.unique_name
        return f"YT-{entity.__name__}-{self.name}"


cdef str short_hash(str val):
    md5 = hashlib.md5(val.encode("UTF-8"))
    return md5.hexdigest()[0:6]
