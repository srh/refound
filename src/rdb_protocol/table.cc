#include "rdb_protocol/table.hpp"

#include "clustering/admin_op_exc.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/auth/user_fut.hpp"
#include "fdb/prov_retry_loop.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rdb_protocol/val.hpp"
#include "fdb/xtore.hpp"

namespace ql {

// TODO: Move to db_table.hpp or something.
scoped<table_t> make_artificial_table(
        artificial_reql_cluster_interface_t *art,
        const provisional_table_id &prov_table);

read_response_t prov_read_with_profile(ql::env_t *env, FDBTransaction *txn,
        const store_key_t &pkey, const table_info &info, cv_check_fut &&cvc) {
    const read_mode_t read_mode = dummy_read_mode;

    PROFILE_STARTER_IF_ENABLED(
        env->profile() == profile_bool_t::PROFILE,
        (read_mode == read_mode_t::OUTDATED ? "Perform outdated read." :
         (read_mode == read_mode_t::DEBUG_DIRECT ? "Perform debug_direct read." :
         (read_mode == read_mode_t::SINGLE ? "Perform read." :
                                             "Perform majority read."))),
        env->trace);

    // TODO: Remove splitter.
    profile::splitter_t splitter(env->trace);
    /* propagate whether or not we're doing profiles */

    /* Do the actual read. */
    read_response_t ret;

    try {
        // TODO: This ignores r.read_mode (as it must).
        // QQQ: Make auth check happen (and abort) as soon as future is ready (but after
        // we check_cv?), not after entire read op.
        auth::fdb_user_fut<auth::read_permission> auth_fut = env->get_user_context().transaction_require_read_permission(txn, info.config->basic.database, info.table_id);
        ret = apply_point_read(txn, std::move(cvc), info.table_id,
                pkey, env->profile(), env->interruptor);
        auth_fut.block_and_check(env->interruptor);
    } catch (const cannot_perform_query_exc_t &e) {
        rfail_datum(ql::base_exc_t::OP_FAILED, "Cannot perform read: %s", e.what());
    } catch (auth::permission_error_t const &error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", error.what());
    }

    /* Append the results of the profile to the current task */
    splitter.give_splits(1, ret.event_log);

    return ret;
}

struct prov_read_result {
    read_response_t resp;
    reqlfdb_config_version cv;
    table_info info;
};

prov_read_result prov_read_real_table(
        env_t *env,
        const provisional_table_id &prov_table,
        store_key_t pkey) {
    prov_read_result ret;
    fdb_error_t loop_err = txn_retry_loop_table(
            env->get_rdb_ctx()->fdb,
            env->get_rdb_ctx()->config_caches.get(),
            env->interruptor,
            prov_table,
            [&](FDBTransaction *txn, table_info &&info, cv_check_fut &&cvc) {
        // TODO: read-only txn
        reqlfdb_config_version tmp_cv = cvc.expected_cv;

        ret.resp = prov_read_with_profile(env, txn, pkey, info, std::move(cvc));
        ret.cv = tmp_cv;
        ret.info = std::move(info);
    });
    rcheck_fdb_datum(loop_err, "reading table");
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

    prov_read_result res = prov_read_real_table(env, prov_table, store_key_t(pval.print_primary()));

    point_read_response_t *p_res = boost::get<point_read_response_t>(&res.resp.response);
    r_sanity_check(p_res);
    auto db = make_counted<db_t>(res.info.config->basic.database,
        prov_table.prov_db.db_name, config_version_checker{res.cv.value});

    std::pair<datum_t, scoped<table_t>> ret;
    ret.first = p_res->data;
    ret.second = make_scoped<table_t>(
        make_counted<real_table_t>(
            res.info.table_id,
            res.cv,
            std::move(res.info.config)),
        std::move(db),
        prov_table.table_name,
        dummy_read_mode,
        prov_table.bt);

    return ret;
}

}  // namespace ql
