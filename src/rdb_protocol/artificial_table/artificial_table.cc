// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/artificial_table/artificial_table.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/auth/user_fut.hpp"
#include "fdb/retry_loop.hpp"
#include "rdb_protocol/artificial_table/backend.hpp"
#include "rdb_protocol/datum_stream/readers.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/table_common.hpp"
#include "rdb_protocol/val.hpp"

/* Determines how many coroutines we spawn for a batched replace or insert. */
// TODO: Remove.
static const int max_parallel_ops = 10;

// artificial_table_fdb_t

/* This is a wrapper around `backend->read_row()` that also performs sanity checking, to
help catch bugs in the backend. */
bool checked_read_row_from_backend(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        artificial_table_fdb_backend_t *backend,
        const ql::datum_t &pval,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        admin_err_t *error_out) {
    // Note that we'll catch the `auth::permission_error_t` that this may throw in the
    // calling function, as that's what we want in `do_single_update` below.
    if (!backend->read_row(txn, user_context, pval, interruptor, row_out, error_out)) {
        return false;
    }

#ifndef NDEBUG
    if (row_out->has()) {
        ql::datum_t pval2 = row_out->get_field(
            datum_string_t(backend->get_primary_key_name()), ql::NOTHROW);
        rassert(pval2.has());
        rassert(pval2 == pval);
    }
#endif

    return true;
}

artificial_table_fdb_t::artificial_table_fdb_t(
        artificial_table_fdb_backend_t *backend)
    : base_table_t(config_version_checker::empty()),
      m_backend(backend),
      m_primary_key_name(backend->get_primary_key_name()) {
}

namespace_id_t artificial_table_fdb_t::get_id() const {
    return m_backend->get_table_id();
}

const std::string &artificial_table_fdb_t::get_pkey() const {
    return m_primary_key_name;
}

ql::datum_t artificial_table_fdb_t::read_row(
        ql::env_t *env,
        ql::datum_t pval, UNUSED read_mode_t read_mode) {
    ql::datum_t row_result;

    try {
        const auth::user_context_t &user_context = env->get_user_context();
        fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb, env->interruptor,
                [&](FDBTransaction *txn) {
            auth::fdb_user_fut<auth::read_permission> auth_fut = m_backend->get_read_permission(txn, user_context);

            // It's an artificial table; just block and check the auth fut up-front,
            // before doing a huge read, other logic like that, possibly failing with
            // its own auth fut.
            auth_fut.block_and_check(env->interruptor);
            admin_err_t error;
            ql::datum_t row;
            if (!checked_read_row_from_backend(
                    txn,
                    user_context,
                    m_backend,
                    pval,
                    env->interruptor,
                    &row,
                    &error)) {
                REQL_RETHROW_DATUM(error);
            }
            row_result = std::move(row);
        });
        guarantee_fdb_TODO(loop_err, "artificial_table_fdb_t::read_row retry loop");
    } catch (auth::permission_error_t const &permission_error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
    }

    guarantee(row_result.has(), "fdb backends never output uninitialized row upon success");
    return row_result;
}

scoped<ql::datum_stream_t> artificial_table_fdb_t::read_all(
        ql::env_t *env,
        const std::string &get_all_sindex_id,
        ql::backtrace_id_t bt,
        const std::string &table_name,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        UNUSED read_mode_t read_mode) {
    scoped<ql::datum_stream_t> stream;

    try {
        if (get_all_sindex_id != m_primary_key_name) {
            rfail_datum(ql::base_exc_t::OP_FAILED, "%s",
                error_message_index_not_found(get_all_sindex_id, table_name).c_str());
        }

        admin_err_t error;
        if (!m_backend->read_all_rows_filtered_as_stream(
                env->get_rdb_ctx()->fdb,
                env->get_user_context(),
                bt,
                datumspec,
                sorting,
                env->interruptor,
                &stream,
                &error)) {
            REQL_RETHROW_DATUM(error);
        }
    } catch (auth::permission_error_t const &permission_error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
    }

    return stream;
}

scoped_ptr_t<ql::reader_t> artificial_table_fdb_t::read_all_with_sindexes(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        read_mode_t read_mode) {
    // This is just a read_all for an artificial table, because sindex is always
    // the primary index. We still need to return a reader_t, this is needed for
    // eq_join.
    r_sanity_check(sindex == get_pkey());
    scoped<ql::datum_stream_t> datum_stream =
        read_all(env, sindex, bt, table_name, datumspec, sorting, read_mode);

    scoped_ptr_t<ql::eager_acc_t> to_array = ql::make_to_array();
    datum_stream->accumulate_all(env, to_array.get());
    ql::datum_t items = to_array->finish_eager(
        bt,
        false,
        ql::configured_limits_t::unlimited)->as_datum(env);

    std::vector<ql::datum_t> items_vector;

    guarantee(items.get_type() == ql::datum_t::type_t::R_ARRAY);
    for (size_t i = 0; i < items.arr_size(); ++i) {
        items_vector.push_back(items.get(i));
    }

    return make_scoped<ql::vector_reader_t>(std::move(items_vector));
}

scoped<ql::datum_stream_t> artificial_table_fdb_t::read_intersecting(
        ql::env_t *env,
        const std::string &sindex,
        UNUSED ql::backtrace_id_t bt,
        const std::string &table_name,
        UNUSED read_mode_t read_mode,
        UNUSED const ql::datum_t &query_geometry) {
    try {
        fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb, env->interruptor,
            [&](FDBTransaction *txn) {
            auth::fdb_user_fut<auth::read_permission> auth_fut
                = m_backend->get_read_permission(txn, env->get_user_context());
            auth_fut.block_and_check(env->interruptor);
        });
        guarantee_fdb_TODO(loop_err, "artificial_table_fdb_t::read_intersecting retry loop");
    } catch (auth::permission_error_t const &permission_error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
    }

    guarantee(
        sindex != m_primary_key_name,
        "read_intersecting() should never be called with the primary index");
    rfail_datum(ql::base_exc_t::OP_FAILED, "%s",
        error_message_index_not_found(sindex, table_name).c_str());
}

ql::datum_t artificial_table_fdb_t::read_nearest(
        ql::env_t *env,
        const std::string &sindex,
        const std::string &table_name,
        UNUSED read_mode_t read_mode,
        UNUSED lon_lat_point_t center,
        UNUSED double max_dist,
        UNUSED uint64_t max_results,
        UNUSED const ellipsoid_spec_t &geo_system,
        UNUSED dist_unit_t dist_unit,
        UNUSED const ql::configured_limits_t &limits) {
    try {
        fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb, env->interruptor,
            [&](FDBTransaction *txn) {
            auth::fdb_user_fut<auth::read_permission> auth_fut
                = m_backend->get_read_permission(txn, env->get_user_context());
            auth_fut.block_and_check(env->interruptor);
        });
        guarantee_fdb_TODO(loop_err, "artificial_table_fdb_t::read_nearest retry loop");
    } catch (auth::permission_error_t const &permission_error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
    }

    guarantee(
        sindex != m_primary_key_name,
        "read_nearest() should never be called with the primary index");
    rfail_datum(ql::base_exc_t::OP_FAILED, "%s",
        error_message_index_not_found(sindex, table_name).c_str());
}

ql::datum_t artificial_table_fdb_t::write_batched_replace(
        ql::env_t *env,
        const std::vector<ql::datum_t> &keys,
        const ql::deterministic_func &func,
        return_changes_t return_changes,
        UNUSED durability_requirement_t durability,
        UNUSED ignore_write_hook_t ignore_write_hook) {
    /* Note that we ignore the `durability` optarg. In theory we could assert that it's
    unspecified or specified to be "soft", since durability is irrelevant or effectively
    soft for system tables anyway. But this might lead to some confusing errors if the
    user passes `durability="hard"` as a global optarg. So we silently ignore it. */

    ql::datum_t stats = ql::datum_t::empty_object();
    std::set<std::string> conditions;
    optional<std::string> permission_error_what;
    throttled_pmap(keys.size(), [&] (int i) {
        try {
            do_single_update(env, keys[i], false,
                [&] (ql::datum_t old_row) {
                    return func->call(env, old_row, ql::eval_flags_t::LITERAL_OK)->as_datum(env);
                },
                return_changes, env->interruptor, &stats, &conditions);
        } catch (auth::permission_error_t const &permission_error) {
            if (!permission_error_what.has_value()) {
                permission_error_what.set(permission_error.what());
            }
        } catch (const interrupted_exc_t &) {
            /* don't throw since we're in throttled_pmap() */
        }
    }, max_parallel_ops);

    if (permission_error_what.has_value()) {
        rfail_datum(
            ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error_what->c_str());
    } else if (env->interruptor->is_pulsed()) {
        throw interrupted_exc_t();
    }

    ql::datum_object_builder_t obj_builder(stats);
    obj_builder.add_warnings(conditions, env->limits());
    return std::move(obj_builder).to_datum();
}

ql::datum_t artificial_table_fdb_t::write_batched_insert(
        ql::env_t *env,
        std::vector<ql::datum_t> &&inserts,
        std::vector<bool> &&pkey_was_autogenerated,
        conflict_behavior_t conflict_behavior,
        optional<ql::deterministic_func> conflict_func,
        return_changes_t return_changes,
        UNUSED durability_requirement_t durability,
        UNUSED ignore_write_hook_t ignore_write_hook) {
    ql::datum_t stats = ql::datum_t::empty_object();
    std::set<std::string> conditions;
    optional<std::string> permission_error_what;
    throttled_pmap(inserts.size(), [&] (int i) {
        try {
            ql::datum_t insert_row = inserts[i];
            ql::datum_t key = insert_row.get_field(
                datum_string_t(m_primary_key_name), ql::NOTHROW);
            guarantee(key.has(), "write_batched_insert() shouldn't ever be called with "
                "documents that lack a primary key.");

            do_single_update(
                env,
                key,
                pkey_was_autogenerated[i],
                [&](ql::datum_t old_row) {
                    return resolve_insert_conflict(
                        env,
                        m_primary_key_name,
                        old_row,
                        insert_row,
                        conflict_behavior,
                        conflict_func);
                },
                return_changes,
                env->interruptor,
                &stats,
                &conditions);
        } catch (auth::permission_error_t const &permission_error) {
            if (!permission_error_what.has_value()) {
                permission_error_what.set(permission_error.what());
            }
        } catch (const interrupted_exc_t &) {
            /* don't throw since we're in throttled_pmap() */
        }
    }, max_parallel_ops);

    if (permission_error_what.has_value()) {
        rfail_datum(
            ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error_what->c_str());
    } else if (env->interruptor->is_pulsed()) {
        throw interrupted_exc_t();
    }

    ql::datum_object_builder_t obj_builder(stats);
    obj_builder.add_warnings(conditions, env->limits());
    return std::move(obj_builder).to_datum();
}

void artificial_table_fdb_t::do_single_update(
        ql::env_t *env,
        ql::datum_t pval,
        bool pkey_was_autogenerated,
        const std::function<ql::datum_t(ql::datum_t)>
            &function,
        return_changes_t return_changes,
        const signal_t *interruptor,
        ql::datum_t *stats_inout,
        std::set<std::string> *conditions_inout) {
    // TODO: Given env isn't used for evaluation, it's p screwy that we pass it in.
    // QQQ: env->get_rdb_ctx() might be null, if we make a pristine env -- but how would we get an artificial_table_fdb_t?
    ql::datum_t resp_out;
    fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb, interruptor,
            [&](FDBTransaction *txn) {
        auth::fdb_user_fut<auth::write_permission> user_fut = env->get_user_context().transaction_require_write_permission(txn, artificial_reql_cluster_interface_t::database_id, m_backend->get_table_id());
        user_fut.block_and_check(env->interruptor);

        admin_err_t error;
        ql::datum_t old_row;
        if (!checked_read_row_from_backend(txn,
                env->get_user_context(), m_backend, pval, interruptor, &old_row, &error)) {
            ql::datum_object_builder_t builder;
            builder.add_error(error.msg.c_str());
            resp_out = std::move(builder).to_datum();
            return;
        }
        if (!old_row.has()) {
            old_row = ql::datum_t::null();
        }

        ql::datum_t resp;
        ql::datum_t new_row;
        try {
            new_row = function(old_row);
            rcheck_row_replacement(
                datum_string_t(m_primary_key_name),
                store_key_t(pval.print_primary()),
                old_row,
                new_row);
            if (new_row.get_type() == ql::datum_t::R_NULL) {
                new_row.reset();
            }

            // TODO: write_row will update the config version, right?
            if (!m_backend->write_row(
                    txn,
                    env->get_user_context(),
                    pval,
                    pkey_was_autogenerated,
                    &new_row,
                    interruptor,
                    &error)) {
                REQL_RETHROW_DATUM(error);
            }
            if (!new_row.has()) {
                new_row = ql::datum_t::null();
            }
            bool dummy_was_changed;
            resp = make_row_replacement_stats(
                datum_string_t(m_primary_key_name),
                store_key_t(pval.print_primary()),
                old_row,
                new_row,
                return_changes,
                &dummy_was_changed);
        } catch (const ql::base_exc_t &e) {
            resp = make_row_replacement_error_stats(
                old_row, new_row, return_changes, e.what());
        }

        resp_out = std::move(resp);
    });
    guarantee_fdb_TODO(loop_err, "do_single_update retry loop");

    *stats_inout = (*stats_inout).merge(
        resp_out, ql::stats_merge, env->limits(), conditions_inout);
}

