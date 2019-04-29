```python

from yapic.entity.sql import Entity, Int, Serial, Auto, String, ForeignKey, DateTimeTz, and_
from yapic.entity.sql import One, Many, ManyAcross


class Address(Entity):
   id: Serial
   addr: String
   deleted_time: DateTimeTz


class Phone(Entity):
   number: String = PrimaryKey()
   user_id: Auto = ForeignKey("User.id")


class Tag(Entity):
   tag: String = PrimaryKey()


class User(Entity):
   id: Serial

   address_id: Auto = ForeignKey(Address.id)
   address: One[Address]
   custom_address: One[Address] = "and_(User.address_id == Address.id, Address.deleted_time.is_null())"

   phone_numbers: Many[Phone]

   tags: ManyAcross["UserTags", Tag]

   custom_tags: ManyAcross["UserTags", Tag] = {
      # describe, how to join UserTags
      "UserTags": "UserTags.user_id == User.id",
      # describe, how to join Tag to UserTags
      Tag: "Tag.tag == UserTags.tag"
   }


class UserTags(Entity):
   user_id: Auto = PrimaryKey() // ForeignKey(User.id)
   tag: Auto = PrimaryKey() // ForeignKey(Tag.tag)

```
