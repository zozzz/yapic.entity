from yapic.entity._entity cimport EntityState
from yapic.entity._query cimport RCO, RowConvertOp
from yapic.entity._entity cimport EntityBase, EntityAttribute
from yapic.entity._field cimport StorageTypeFactory, StorageType, Field

from cpython.ref cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_GET_ITEM, PyTuple_SET_ITEM, PyTuple_GET_SIZE


cdef class RCState:
    def __cinit__(self, object conn):
        self.conn = conn
        self.cache = {}


async def convert_record(object record, list rcos_list, RCState state):
    cdef int rcos_list_len = len(rcos_list)
    cdef tuple converted = PyTuple_New(rcos_list_len)
    cdef list rcos
    cdef RowConvertOp rco
    cdef list stack = []
    cdef object result
    cdef object tmp
    cdef EntityState entity_state
    cdef EntityAttribute attr
    cdef Field field
    cdef StorageType stype
    cdef StorageTypeFactory tf = state.conn.dialect.create_type_factory()

    push = stack.append
    pop = stack.pop

    for i in range(0, rcos_list_len):
        rcos = <list>(<list>(rcos_list)[i])

        for j in range(0, len(rcos)):
            rco = <RowConvertOp>(<list>(rcos)[j])

            if rco.op == RCO.PUSH:
                push(result)
            elif rco.op == RCO.POP:
                tmp = pop()
            elif rco.op == RCO.CREATE_STATE:
                entity_state = EntityState(rco.param1)
            elif rco.op == RCO.CREATE_ENTITY:
                result = rco.param1(entity_state)
            elif rco.op == RCO.CREATE_POLYMORPH_ENTITY:
                pass
            elif rco.op == RCO.LOAD_ENTITY:
                pass
            elif rco.op == RCO.SET_ATTR:
                entity_state.set_initial_value(<EntityAttribute>rco.param1, tmp)
            elif rco.op == RCO.SET_ATTR_RECORD:
                field = rco.param1
                tmp = record[rco.param2]
                if tmp is None:
                    entity_state.set_initial_value(field, None)
                else:
                    stype = field.get_type(tf)
                    entity_state.set_initial_value(field, stype.decode(tmp))
            elif rco.op == RCO.GET_RECORD:
                result = record[rco.param1]

        Py_INCREF(<object>result)
        PyTuple_SET_ITEM(<object>converted, i, <object>result)

    if rcos_list_len == 1:
        return converted[0]
    else:
        return converted



