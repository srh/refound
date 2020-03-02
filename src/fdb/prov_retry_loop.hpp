#ifndef RETHINKDB_FDB_PROV_RETRY_LOOP_HPP_
#define RETHINKDB_FDB_PROV_RETRY_LOOP_HPP_

#include "clustering/tables/table_metadata.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"
#include "rdb_protocol/table.hpp"

struct table_info {
    namespace_id_t table_id;
    counted<const rc_wrapper<table_config_t>> config;
};

// Note that this function can't be used on system tables.
template <class C>
MUST_USE fdb_error_t txn_retry_loop_table(
        FDBDatabase *fdb, reqlfdb_config_cache *cc, const signal_t *interruptor,
        const provisional_table_id &prov_table,
        C &&fn) {
    static_assert(std::is_same<void, decltype(fn(std::declval<FDBTransaction *>(), std::declval<table_info &&>(), std::declval<cv_check_fut&&>()))>::value,
        "txn_retry_loop_table fn has wrong operator() signature");

    rassert(prov_table.prov_db.db_name != artificial_reql_cluster_interface_t::database_name);
    fdb_transaction txn{fdb};

    optional<config_info<database_id_t>>
        cached_db = cc->try_lookup_cached_db(prov_table.prov_db.db_name);

    optional<config_info<std::pair<namespace_id_t, counted_t<const rc_wrapper<table_config_t>>>>>
        cached_table;

    if (cached_db.has_value()) {
        cached_table = cc->try_lookup_cached_table(std::make_pair(cached_db->ci_value, prov_table.table_name));
    }

    for (;;) {
        try {
            // If there is a cached value, the first time through, we try using it, but
            // we have to check (in the txn) that the config version hasn't changed.
            if (cached_table.has_value()) {
                cv_check_fut cvc;
                cvc.cv_fut = transaction_get_config_version(txn.txn);
                cvc.expected_cv = cached_table->ci_cv;
                table_info info;
                info.table_id = cached_table->ci_value.first;
                info.config = cached_table->ci_value.second;
                fn(txn.txn, std::move(info), std::move(cvc));
            } else {
                config_info<std::pair<namespace_id_t, table_config_t>>
                    table = expect_retrieve_table(txn.txn, prov_table, interruptor);

                // Update the cache.
                cc->note_version(table.ci_cv);
                cc->add_db(table.ci_value.second.basic.database, prov_table.prov_db.db_name);
                auto ptr = make_counted<const rc_wrapper<table_config_t>>(std::move(table.ci_value.second));
                cc->add_table(table.ci_value.first, ptr);

                table_info info;
                info.table_id = table.ci_value.first;
                info.config = std::move(ptr);
                cv_check_fut cvc;
                cvc.expected_cv = table.ci_cv;
                fn(txn.txn, std::move(info), std::move(cvc));
            }
        } catch (const provisional_assumption_exception &exc) {
            guarantee(cached_table.has_value());
            cached_table.reset();
            // Might as well wipe the cache if applicable.
            cc->note_version(exc.cv);
        } catch (const fdb_transaction_exception &exc) {
            fdb_error_t orig_err = exc.error();
            if (orig_err == REQLFDB_commit_unknown_result) {
                // From fdb documentation:  if there is an unknown result,
                // the txn must be an idempotent operation.
                return orig_err;
            }

            fdb_future fut{fdb_transaction_on_error(txn.txn, orig_err)};
            // The exponential backoff strategy is what blocks the coro.
            fut.block_coro(interruptor);
            fdb_error_t err = fdb_future_get_error(fut.fut);
            if (err != 0) {
                // TODO: Remove this guarantee.
                guarantee(err == exc.error());
                return err;
            }
        }
    }
}

// Used when we don't want the table config, we just want the db id / table id.  This
// will work with the artificial database, too.
template <class C>
MUST_USE fdb_error_t txn_retry_loop_table_id(
        FDBDatabase *fdb, reqlfdb_config_cache *cc, const signal_t *interruptor,
        const provisional_table_id &prov_table,
        C &&fn) {
    fdb_transaction txn{fdb};

    // NNN: If we use this function on the artificial db, there is trouble.

    optional<namespace_id_t> artificial_table_id;
    if (prov_table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
        artificial_table_id = artificial_reql_cluster_interface_t::get_table_id(prov_table.table_name);
        if (!artificial_table_id.has_value()) {
            rfail_prov_table_dne(prov_table);
        }
    }

    optional<config_info<database_id_t>>
        cached_db;
    optional<config_info<std::pair<namespace_id_t, counted_t<const rc_wrapper<table_config_t>>>>>
        cached_table;

    if (!artificial_table_id.has_value()) {
        cached_db = cc->try_lookup_cached_db(prov_table.prov_db.db_name);

        if (cached_db.has_value()) {
            // NNN: Just lookup the table id, not the config.
            cached_table = cc->try_lookup_cached_table(std::make_pair(cached_db->ci_value, prov_table.table_name));
        }
    }

    for (;;) {
        try {
            // If there is a cached value, the first time through, we try using it, but
            // we have to check (in the txn) that the config version hasn't changed.
            if (cached_table.has_value()) {
                cv_check_fut cvc;
                cvc.cv_fut = transaction_get_config_version(txn.txn);
                cvc.expected_cv = cached_table->ci_cv;
                fn(txn.txn, cached_db->ci_value, cached_table->ci_value.first, std::move(cvc));
            } else {
                namespace_id_t table_id;
                database_id_t db_id;
                if (artificial_table_id.has_value()) {
                    table_id = *artificial_table_id;
                    db_id = artificial_reql_cluster_interface_t::database_id;
                } else {
                    // NNN: We should only load the db id and table id (which is one less round-trip).
                    config_info<std::pair<namespace_id_t, table_config_t>>
                        table = expect_retrieve_table(txn.txn, prov_table, interruptor);
                    table_id = table.ci_value.first;
                    db_id = table.ci_value.second.basic.database;
                }
                fn(txn.txn, db_id, table_id, cv_check_fut());
            }
        } catch (const provisional_assumption_exception &exc) {
            guarantee(cached_table.has_value());
            cached_table.reset();
            // Might as well wipe the cache if applicable.
            cc->note_version(exc.cv);
        } catch (const fdb_transaction_exception &exc) {
            fdb_error_t orig_err = exc.error();
            if (orig_err == REQLFDB_commit_unknown_result) {
                // From fdb documentation:  if there is an unknown result,
                // the txn must be an idempotent operation.
                return orig_err;
            }

            fdb_future fut{fdb_transaction_on_error(txn.txn, orig_err)};
            // The exponential backoff strategy is what blocks the coro.
            fut.block_coro(interruptor);
            fdb_error_t err = fdb_future_get_error(fut.fut);
            if (err != 0) {
                // TODO: Remove this guarantee.
                guarantee(err == exc.error());
                return err;
            }
        }
    }
}


#endif  // RETHINKDB_FDB_PROV_RETRY_LOOP_HPP_
