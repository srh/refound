// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "rpc/connectivity/server_id.hpp"

#include "containers/printf_buffer.hpp"

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
