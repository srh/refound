#include "protocol_api.hpp"

#include "arch/runtime/coroutines.hpp"

RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(cannot_perform_query_exc_t, message, query_state);
