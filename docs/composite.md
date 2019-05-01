```python

from yapic.entity import Entity, Int, String, Serial, Composite
from yapic.entity.sql import Entity as SqlEntity


class FullName(Entity):
   title: String
   family: String
   given: String

   @property
   def full(self):
      return " ".join(filter(bool, [self.title, self.family, self.given]))


class User(SqlEntity):
   id: Serial
   name: Composite[FullName]

```
