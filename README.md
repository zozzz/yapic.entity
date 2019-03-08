# yapic.entity


Example:
```python

class User(Entity):
    id: Int = PrimaryKey()
    name: String = Field(size=100)
    with_default: String = "Default value"
    roles: Many["UserRole"]
    tags: ManyAcross["UserTags", "Tag"]


class UserRole(Entity):
    id: Int = PrimaryKey()
    user_id: Int = ForeignKey(User.id)
    user: One[User]

class Tag(Entity):
   id: Int
   value: String


class UserTags(Entity):
   user_id: Int = ForeignKey(User.id)
   tag_id: Int = ForeignKey(Tag.id)


```

Field Extensions:
```python


class Unique(Validate):
   def __init__(self):
      super().__init__(self.__check)

   def __check(self, value, field, entity):
      return False  # not unique


class Email(Validate):
   def __init__(self):
      super().__init__(lambda v: True)


class User(Entity):
    id: Int = Field() // PrimaryKey()
    name: String = Field(size=[3, 100]) \
        // Validate(lambda value, field, entity: 3 < len(value) < 100, "Error ...") \
        // Validate("(3 < name.len() < 100) & name != 'jhon'", "Error ...") \
        // Unique()
    email: String = Field(size=255) \
        // Empty() | Email()     # optional valid email
        // ~Empty() & Email()    # required valid email


```

Field Access:
```python

class User(Entity):
    id: Int = PrimaryKey()
    name: String
    email: String
    password: String


    class access:
        class owner(FieldAccess.READ_WRITE):
            pass

        class common(FieldAccess.READ):
            password = None

        class guest(FieldAccess.NONE):
            pass

        def select(self, model):
            if model and model.id == current_user_id():
                return self.owner

            if current_user_id():
                return self.common

            return self.guest
            # or just simply return
            return FieldAccess.NONE

        # this si just an example, if u want orm features use yapic.orm
        def update_query(self, query):
            if not current_user_id():
                return raw_query("SELECT 0 WHERE FALSE")
            return query

```
