```python

from yapic.entity.sql import Entity, Int, Serial, Auto, String


class Employee(Entity, polymorph="variant"):
   id: Serial
   variant: String
   some_field: String


class Manager(Employee, polymorph_id="manager"):
   # automatically inherit primary key, and make foreign key
   # id: Auto = ForeignKey(PolyBase.id, on_update="CASCADE", on_delete="CACADE") // PrimaryKey()
   another_field: String


class Worker(Employee, polymorph_id="worker"):
   # automatically inherit primary key, and make foreign key
   # id: Auto = ForeignKey(PolyBase.id, on_update="CASCADE", on_delete="CACADE") // PrimaryKey()
   xyz: String


```
