import cython
from cpython.ref cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_GET_ITEM, PyTuple_SET_ITEM, PyTuple_GET_SIZE

from yapic.entity._entity cimport EntityState
from yapic.entity._query cimport RCO, RowConvertOp
from yapic.entity._entity cimport EntityBase, EntityAttribute
from yapic.entity._field cimport StorageTypeFactory, StorageType, Field




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
    cdef int j
    cdef int rcos_len

    push = stack.append
    pop = stack.pop

    for i in range(0, rcos_list_len):
        rcos = <list>(<list>(rcos_list)[i])
        j = 0
        rcos_len = len(rcos)

        # for j in range(0, len(rcos)):
        while j < rcos_len:
            rco = <RowConvertOp>(<list>(rcos)[j])
            # print(rco)

            if rco.op == RCO.PUSH:
                push(result)
            elif rco.op == RCO.POP:
                tmp = pop()
            elif rco.op == RCO.JUMP:
                j = rco.param1
                continue
            elif rco.op == RCO.CREATE_STATE:
                entity_state = EntityState(rco.param1)
                entity_state.exists = True
            elif rco.op == RCO.CREATE_ENTITY:
                result = rco.param1(entity_state)
            elif rco.op == RCO.CREATE_POLYMORPH_ENTITY:
                # XXX: Debug Only! All this error is internal error only
                # if not isinstance(rco.param1, tuple):
                #     raise RuntimeError("Invalid param1 for RCO: %r" % rco)

                # if not isinstance(rco.param2, dict):
                #     raise RuntimeError("Invalid param1 for RCO: %r" % rco)

                poly_id = _record_idexes_to_tuple(<tuple>(rco.param1), record)
                poly_jump = (<dict>rco.param2).get(poly_id, None)
                if poly_jump is not None:
                    j = poly_jump
                    continue
            elif rco.op == RCO.LOAD_ONE_ENTITY:
                related_id = _record_idexes_to_tuple(<tuple>(rco.param1), record)
                query = rco.param2(related_id)
                push(await state.conn.select(query).first())
            elif rco.op == RCO.LOAD_MULTI_ENTITY:
                related_id = _record_idexes_to_tuple(<tuple>(rco.param1), record)
                query = rco.param2(related_id)
                push(await state.conn.select(query))
            elif rco.op == RCO.SET_ATTR:
                entity_state.set_initial_value(<EntityAttribute>rco.param1, tmp)
            elif rco.op == RCO.SET_ATTR_RECORD:
                field = rco.param1
                tmp = record[rco.param2]
                if tmp is None:
                    entity_state.set_initial_value(field, None)
                else:
                    stype = field.get_type(tf)
                    tmp = stype.decode(tmp)
                    entity_state.set_initial_value(field, tmp)
            elif rco.op == RCO.GET_RECORD:
                result = record[rco.param1]

            j += 1

        Py_INCREF(<object>result)
        PyTuple_SET_ITEM(<object>converted, i, <object>result)

    if rcos_list_len == 1:
        return converted[0]
    else:
        return converted


cdef tuple _record_idexes_to_tuple(tuple idx_list, object record):
    cdef tuple result
    cdef int length = len(idx_list)

    if length == 1:
        return (record[idx_list[0]],)
    else:
        result = PyTuple_New(length)
        for i in range(0, length):
            val = record[idx_list[i]]
            Py_INCREF(<object>val)
            PyTuple_SET_ITEM(<object>result, i, <object>val)
        return result
