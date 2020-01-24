#include "clustering/administration/tables/split_points.hpp"

#include "clustering/administration/real_reql_cluster_interface.hpp"


// TODO: Remove if unused.
void fetch_distribution(
        const namespace_id_t &table_id,
        real_reql_cluster_interface_t *reql_cluster_interface,
        signal_t *interruptor,
        std::map<store_key_t, int64_t> *counts_out)
        THROWS_ONLY(interrupted_exc_t, failed_table_op_exc_t, no_such_table_exc_t) {
    namespace_interface_access_t ns_if_access =
        reql_cluster_interface->get_namespace_repo()->get_namespace_interface(
            table_id, interruptor);
    static const int depth = 2;
    static const int limit = 128;
    distribution_read_t inner_read(depth, limit);
    read_t read(inner_read, profile_bool_t::DONT_PROFILE, read_mode_t::OUTDATED);
    read_response_t resp;
    try {
        ns_if_access.get()->read(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::False, tribool::False, tribool::False)),
            read,
            &resp,
            order_token_t::ignore,
            interruptor);
    } catch (const cannot_perform_query_exc_t &) {
        /* If the table was deleted, this will throw `no_such_table_exc_t` */
        table_basic_config_t dummy;
        reql_cluster_interface->get_table_meta_client()->get_name(table_id, &dummy);
        /* If `get_name()` didn't throw, the table exists but is inaccessible */
        throw failed_table_op_exc_t();
    }
    *counts_out = std::move(
        boost::get<distribution_read_response_t>(resp.response).key_counts);
}
