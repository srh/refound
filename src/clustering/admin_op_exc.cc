#include "clustering/admin_op_exc.hpp"

#include "fdb/reql_fdb.hpp"

bool help_set_fdb_error(fdb_error_t err, admin_err_t *error_out, const char *msg) {
    if (err == 0) {
        return false;
    }
    if (op_indeterminate(err)) {
        // TODO: Better messages, using fdb error code.
        error_out->msg = (std::string(msg) + ": FoundationDB transaction may have failed: ") + fdb_get_error(err);
        error_out->query_state = query_state_t::INDETERMINATE;
    } else {
        error_out->msg = (std::string(msg) + ": FoundationDB transaction failed: ") + fdb_get_error(err);
        error_out->query_state = query_state_t::FAILED;
    }
    return true;
}
