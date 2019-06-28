```python


class FullName(Entity):
    title: String
    family: String
    given: String


class User(Entity):
    id: Serial
    name: FullName

    @virtual
    def full_name(self):
        return " ".join(filter(bool, (self.title, self.family, self.given)))

    @full_name.compare
    def full_name(cls, query: Query, op: callable, value: Any):
        if op is __eq__:
            return cls.full_name._val(cls, query)_ == value

        return or_(
           op(cls.name.family, value),
           op(cls.name.given, value))

   @full_name.value
   def full_name(cls, query: Query):
      return func.CONCAT_WS(" ", cls.name.title, cls.name.family, cls.name.given)

   @full_name.order
   def full_name_order(cls, q: Query, op):
      return op(func.CONCAT_WS(" ", cls.family, cls.given))


q = Query(User).where(User.full_name == "John Doe")
```
