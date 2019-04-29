```python

from yapic.entity.sql import Entity, Serial, String


class Gender(Entity):
   id: String = PrimaryKey()


Gender.__fix_entries__ = [
   Gender(id="male"),
   Gender(id="female"),
   Gender(id="other"),
]

```
