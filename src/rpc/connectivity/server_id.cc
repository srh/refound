// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "rpc/connectivity/server_id.hpp"

std::string server_id_t::print() const {
    return uuid_to_str(get_uuid());
}

bool str_to_server_id(const std::string &in, server_id_t *out) {
    if (in != server_id_t().print()) {
        return false;
    }
    *out = server_id_t();
    return true;
}

void debug_print(printf_buffer_t *buf, const server_id_t &server_id) {
    buf->appendf("%s", server_id.print().c_str());
}

template <cluster_version_t W>
void serialize(write_message_t *, const server_id_t &) {
}
template <cluster_version_t W>
archive_result_t deserialize(read_stream_t *, server_id_t *) {
    return archive_result_t::SUCCESS;
}
INSTANTIATE_SERIALIZABLE_SINCE_v2_5(server_id_t);
