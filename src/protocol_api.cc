#include "protocol_api.hpp"

#include "arch/runtime/coroutines.hpp"

RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(cannot_perform_query_exc_t, message, query_state);

#if RDB_CF
namespace_interface_access_t::namespace_interface_access_t() {}

namespace_interface_access_t::namespace_interface_access_t(
        const namespace_id_t &table_id) :
    nsi_(std::make_shared<table_query_client_t>(table_id))
{
}

namespace_interface_access_t::~namespace_interface_access_t() {}
#endif  // RDB_CF
