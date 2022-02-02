import cython
from cpython.ref cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_GET_ITEM, PyTuple_SET_ITEM, PyTuple_GET_SIZE

from yapic.entity._entity cimport EntityState
from yapic.entity._entity cimport EntityBase, EntityAttribute
from yapic.entity._field cimport StorageTypeFactory, StorageType, Field

from ._query cimport RCO, RowConvertOp



cdef class RCState:
    def __cinit__(self, object conn):
        self.conn = conn
        self.cache = {}
        self.tf = conn.dialect.create_type_factory()


def convert_record(object record, list rcos_list, RCState state):
    return _convert_record([], record, rcos_list, state)


cdef object _convert_record(list stack, object record, list rcos_list, RCState state):
    cdef int rcos_list_len = len(rcos_list)
    cdef tuple converted = PyTuple_New(rcos_list_len)
    cdef list rcos
    cdef RowConvertOp rco
    cdef object result = None
    cdef object tmp
    cdef EntityState entity_state = None
    cdef Field field = None
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
            # print(">", rco)

            if rco.op == RCO.PUSH:
                push(result)
                # print("push", stack)
            elif rco.op == RCO.POP:
                tmp = pop()
                # print("pop", tmp, stack)
            elif rco.op == RCO.CREATE_STATE:
                entity_state = EntityState(rco.param1)
                entity_state.exists = True
            elif rco.op == RCO.CREATE_ENTITY:
                # print("CREATE_ENTITY", rco.param1, entity_state._is_empty())
                if rco.param2 is True and entity_state._is_empty() is True:
                    result = None
                else:
                    result = rco.param1(entity_state)
            elif rco.op == RCO.CREATE_POLYMORPH_ENTITY:
                # XXX: Debug Only! All this error is internal error only
                # if not isinstance(rco.param1, tuple):
                #     raise RuntimeError("Invalid param1 for RCO: %r" % rco)

                # if not isinstance(rco.param2, dict):
                #     raise RuntimeError("Invalid param1 for RCO: %r" % rco)

                poly_id = _record_idexes_to_tuple(<tuple>(rco.param1), record)
                try:
                    poly_rco = (<dict>rco.param2)[poly_id]
                except KeyError:
                    result = pop()
                else:
                    result = _convert_record(stack, record, poly_rco, state)
            elif rco.op == RCO.CONVERT_SUB_ENTITY:
                tmp = record[<int>rco.param1]
                if tmp is not None:
                    result = _convert_record(stack, tmp, rco.param2, state)
                else:
                    result = None
            elif rco.op == RCO.CONVERT_SUB_ENTITIES:
                tmp = record[<int>rco.param1]
                result = []
                if tmp:
                    for entry in tmp:
                        entity = _convert_record(stack, entry, rco.param2, state)
                        if entity is not None:
                            result.append(entity)
            elif rco.op == RCO.SET_ATTR:
                entity_state.set_initial_value(<EntityAttribute>rco.param1, tmp)
            elif rco.op == RCO.SET_ATTR_RECORD:
                field = rco.param1
                tmp = record[rco.param2]
                if tmp is None:
                    entity_state.set_initial_value(field, None)
                else:
                    tmp = field.get_type(state.tf).decode(tmp)
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
    cdef int length = len(idx_list)
    cdef tuple result = PyTuple_New(length)

    for i in range(0, length):
        val = record[idx_list[i]]
        Py_INCREF(<object>val)
        PyTuple_SET_ITEM(<object>result, i, <object>val)
    return result
