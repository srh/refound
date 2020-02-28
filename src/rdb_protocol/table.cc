#include "rdb_protocol/table.hpp"

#include "rdb_protocol/val.hpp"

namespace ql {

std::pair<datum_t, counted<table_t>> prov_read_row(
        env_t *env,
        const provisional_table_id &prov_table,
        const datum_t &pval) {
    // NNN: No, do the get_row and de-provisionalization of the table in the same fdb txn.
    std::pair<datum_t, counted<table_t>> ret;
    ret.second = provisional_to_table(env, prov_table);
    ret.first = ret.second->get_row(env, pval);
    return ret;
}

}  // namespace ql
