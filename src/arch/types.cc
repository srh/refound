#include "arch/types.hpp"

#include "arch/address.hpp"
#include "utils.hpp"

tcp_socket_exc_t::tcp_socket_exc_t(int errsv, port_t port) {
    info = strprintf("TCP socket creation failed for port %d: %s",
                     port.value, errno_string(errsv).c_str());
}


address_in_use_exc_t::address_in_use_exc_t(const char* hostname, port_t port) throw () {
    if (port.value == 0) {
        info = strprintf("Could not establish sockets on all selected local addresses using the same port");
    } else {
        info = strprintf("The address at %s:%d is reserved or already in use", hostname, port.value);
    }
}

void linux_iocallback_t::on_io_failure(int errsv, int64_t offset, int64_t count) {
    if (errsv == ENOSPC) {
        // fail_due_to_user_error rather than crash because we don't want to
        // print a backtrace in this case.
        fail_due_to_user_error("Ran out of disk space. (offset = %" PRIi64
                               ", count = %" PRIi64 ")", offset, count);
    } else {
        crash("I/O operation failed. (%s) (offset = %" PRIi64 ", count = %" PRIi64 ")",
              errno_string(errsv).c_str(), offset, count);
    }
}
