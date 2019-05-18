```python

class User(Entity):
    id: Serial
    name: String
    attrs: Mapping[UserAttr]


class UserAttrType(Entity):
    id: String = PrimaryKey()
    label: String


UserAttrType.__fix_entries__ = [
    UserAttrType(id="text", label="Text"),
    UserAttrType(id="date", label="Date"),
]


class UserAttrDef(Entity):
    id: String = PrimaryKey()
    type: Auto = ForeignKey(UserAttrType.id)
    label: String


UserAttrDef.__fix_entries__ = [
    UserAttrDef(id="birth_date", type="date", label="Birth date"),
    UserAttrDef(id="introduction", type="text", label="Introduction"),
    UserAttrDef(id="text_attr2", type="text", label="Text attr 2"),
]


class UserAttr(Entity, polymorph="type"):
    user_id: Auto = ForeignKey(User.id) // PrimaryKey()
    key: Auto = ForeignKey(UserAttrDef.id) // PrimaryKey()
    attr: One[UserAttrDef]
    type: RelatedAttr = "attr", "type"


class UserAttrDate(UserAttr, polymorph_id="date"):
    value: Date

class UserAttrText(UserAttr, polymorph_id="text"):
    value: String


```

# Get user birth_date

```sql
SELECT UserAttrDate.value
FROM UserAttr
   INNER JOIN UserAttrDate ON UserAttrDate.user_id = UserAttr.user_id
WHERE UserAttr.user_id = 1 AND UserAtrr.key = 'birth_date'
```


# Get all attributes

```sql
SELECT *
FROM User
   LEFT JOIN

```
