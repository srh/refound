#include "rdb_protocol/table.hpp"

#include "clustering/admin_op_exc.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "fdb/prov_retry_loop.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rdb_protocol/val.hpp"

namespace ql {

// TODO: Move to db_table.hpp or something.
scoped<table_t> make_artificial_table(
        artificial_reql_cluster_interface_t *art,
        const provisional_table_id &prov_table);
read_mode_t dummy_read_mode();

read_response_t prov_read_with_profile(ql::env_t *env, FDBTransaction *txn,
        const read_t &read, const table_info &info, cv_check_fut &&cvc) {
    PROFILE_STARTER_IF_ENABLED(
        env->profile() == profile_bool_t::PROFILE,
        (read.read_mode == read_mode_t::OUTDATED ? "Perform outdated read." :
         (read.read_mode == read_mode_t::DEBUG_DIRECT ? "Perform debug_direct read." :
         (read.read_mode == read_mode_t::SINGLE ? "Perform read." :
                                                  "Perform majority read."))),
        env->trace);
    // TODO: Remove splitter.
    profile::splitter_t splitter(env->trace);
    /* propagate whether or not we're doing profiles */
    r_sanity_check(read.profile == env->profile());

    /* Do the actual read. */
    read_response_t ret;

    try {
        ret = table_query_client_read(
            txn,
            std::move(cvc),
            info.table_id,
            *info.config,
            env->get_user_context(),
            read,
            env->interruptor);
    } catch (const cannot_perform_query_exc_t &e) {
        rfail_datum(ql::base_exc_t::OP_FAILED, "Cannot perform read: %s", e.what());
    } catch (auth::permission_error_t const &error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", error.what());
    }

    /* Append the results of the profile to the current task */
    splitter.give_splits(1, ret.event_log);

    return ret;
}

std::pair<datum_t, scoped<table_t>> prov_read_row(
        env_t *env,
        const provisional_table_id &prov_table,
        const datum_t &pval) {
    if (prov_table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
        artificial_reql_cluster_interface_t *art = env->get_rdb_ctx()->artificial_interface_or_null;
        r_sanity_check(art != nullptr);
        scoped<table_t> table = make_artificial_table(art, prov_table);
        datum_t row = table->get_row(env, pval);
        std::pair<datum_t, scoped<table_t>> ret = {
            std::move(row),
            std::move(table)
        };
        return ret;
    }

    read_t read(point_read_t(store_key_t(pval.print_primary())),
                env->profile(), dummy_read_mode());

    std::pair<datum_t, scoped<table_t>> ret;
    fdb_error_t loop_err = txn_retry_loop_table(
            env->get_rdb_ctx()->fdb,
            env->get_rdb_ctx()->config_caches.get(),
            env->interruptor,
            prov_table,
            [&](FDBTransaction *txn, table_info &&info, cv_check_fut &&cvc) {
        // TODO: Duplicates code from real_table_t::read_row().
        reqlfdb_config_version cv = cvc.expected_cv;

        read_response_t res = prov_read_with_profile(env, txn, read, info, std::move(cvc));
        point_read_response_t *p_res = boost::get<point_read_response_t>(&res.response);
        r_sanity_check(p_res);
        auto db = make_counted<db_t>(info.config->basic.database,
            prov_table.prov_db.db_name, config_version_checker{cv.value});

        ret.first = p_res->data;
        ret.second = make_scoped<table_t>(
            make_counted<real_table_t>(
                info.table_id,
                cv,
                std::move(info.config)),
            std::move(db),
            prov_table.table_name,
            dummy_read_mode(),
            prov_table.bt);
    });
    guarantee_fdb_TODO(loop_err, "prov_read_row retry loop");

    return ret;
}

}  // namespace ql