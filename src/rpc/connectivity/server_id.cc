// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "rpc/connectivity/server_id.hpp"

server_id_t server_id_t::generate_server_id() {
    server_id_t res;
    res.uuid = generate_uuid();
    return res;
}

server_id_t server_id_t::from_server_uuid(uuid_u _uuid) {
    server_id_t res;
    res.uuid = _uuid;
    return res;
}

std::string server_id_t::print() const {
    return uuid_to_str(uuid);
}

bool str_to_server_id(const std::string &in, server_id_t *out) {
    std::string uuid_str = in;
    uuid_u uuid;
    if (!str_to_uuid(uuid_str, &uuid)) {
        return false;
    }
    *out = server_id_t::from_server_uuid(uuid);
    return true;
}

void debug_print(printf_buffer_t *buf, const server_id_t &server_id) {
    buf->appendf("%s", server_id.print().c_str());
}

// Universal serialization functions: you MUST NOT change their implementations.
// (You could find a way to remove these functions, though.)
void serialize_universal(write_message_t *wm, const server_id_t &server_id) {
    // We take the UUID and flag it in one of the reserved bits of version 4 UUIDs,
    // depending on whether the ID is for a proxy or not.
    // The reason for this is so that we don't break compatibility with older servers
    // that expect a plain `uuid_u`.
    uuid_u flagged_uuid = server_id.get_uuid();
    serialize_universal(wm, flagged_uuid);
}
archive_result_t deserialize_universal(read_stream_t *s, server_id_t *server_id) {
    uuid_u flagged_uuid;
    archive_result_t res = deserialize_universal(s, &flagged_uuid);
    if (bad(res)) { return res; }
    *server_id = server_id_t::from_server_uuid(flagged_uuid);
    return archive_result_t::SUCCESS;
}

template <cluster_version_t W>
void serialize(write_message_t *wm, const server_id_t &sid) {
    serialize_universal(wm, sid);
}
template <cluster_version_t W>
archive_result_t deserialize(read_stream_t *s, server_id_t *sid) {
    return deserialize_universal(s, sid);
}
INSTANTIATE_SERIALIZABLE_SINCE_v1_13(server_id_t);
