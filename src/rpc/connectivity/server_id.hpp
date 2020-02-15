// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef RPC_CONNECTIVITY_SERVER_ID_HPP_
#define RPC_CONNECTIVITY_SERVER_ID_HPP_

#include <string>

#include "containers/uuid.hpp"
#include "rpc/serialize_macros.hpp"

/* `server_id_t` is a `uuid_u`, but now there's just one value...

Soon it won't exist. */
class server_id_t {
public:
    // There is now only one server id value.
    server_id_t() { }

    bool operator<(const server_id_t &) const {
        return false;
    }
    bool operator==(const server_id_t &) const {
        return true;
    }
    bool operator!=(const server_id_t &p) const {
        return !(p == *this);
    }

    uuid_u get_uuid() const {
        return str_to_uuid("07ca73b5-80de-4f78-b7f7-dfa5fab59512");
    }

    std::string print() const;

    RDB_DECLARE_ME_SERIALIZABLE(server_id_t);
};

// Inverse of `server_id_t::print`.
bool str_to_server_id(const std::string &in, server_id_t *out);

void debug_print(printf_buffer_t *buf, const server_id_t &server_id);

#endif /* RPC_CONNECTIVITY_SERVER_ID_HPP_ */

