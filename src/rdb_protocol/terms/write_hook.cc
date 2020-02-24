// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/write_hook.hpp"

#include <string>

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/auth/user_fut.hpp"
// For utility function, maybe move convert_write_hook_to_datum out.
#include "clustering/administration/tables/table_config.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/retry_loop.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/op.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/term_walker.hpp"
#include "rdb_protocol/terms/terms.hpp"

std::string format_write_hook_query(const write_hook_config_t &config) {
    std::string ret = "setWriteHook(";
    ret += config.func.det_func.compile_wire_func()->print_js_function();
    ret += ")";
    return ret;
}

namespace ql {

class set_write_hook_term_t final : public op_term_t {
public:
    set_write_hook_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2)) { }

    deterministic_t is_deterministic() const override {
        return deterministic_t::no();
    }

    scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const override {
        provisional_table_id table = args->arg(env, 0)->as_prov_table(env->env);

        // TODO: Maybe we actually want to ping fdb and see if the table still exists
        // before we consider emitting any of the other errors first.  That would more
        // precisely preserve prior behavior in terms of what errors take precedence.

        /* Parse the write_hook configuration */
        optional<write_hook_config_t> config;
        bool deletion_message = true;
        scoped_ptr_t<val_t> v = args->arg(env, 1);
        // RSI: Old reql versions hanging around, being unused, is pretty bad.
        // RSI: Something about write hooks not being specified vs. being specified as "null" in certain API's was weird.

        // We ignore the write_hook's old `reql_version` and make the new version
        // just be `reql_version_t::LATEST`; but in the future we may have
        // to do some conversions for compatibility.
        if (v->get_type().is_convertible(val_t::type_t::DATUM)) {
            datum_t d = v->as_datum(env);
            if (d.get_type() == datum_t::R_BINARY) {
                ql::wire_func_t func;

                datum_string_t str = d.as_binary();
                size_t sz = str.size();
                size_t prefix_sz = strlen(write_hook_blob_prefix);
                const char *data = str.data();
                bool bad_prefix = (sz < prefix_sz);
                for (size_t i = 0; !bad_prefix && i < prefix_sz; ++i) {
                    bad_prefix |= (data[i] != write_hook_blob_prefix[i]);
                }
                rcheck(!bad_prefix,
                       base_exc_t::LOGIC,
                       "Cannot create a write hook except from a reql_write_hook_function"
                       " returned from `get_write_hook`.");

                string_read_stream_t rs(str.to_std(), prefix_sz);
                deserialize<cluster_version_t::LATEST_DISK>(&rs, &func);

                // We check the deterministic func condition immediately below.
                config.set(write_hook_config_t(ql::deterministic_func{func}, reql_version_t::LATEST));
                goto config_specified_with_value;
            } else if (d.get_type() == datum_t::R_NULL) {
                goto config_specified_without_value;
            }
        }

        // This way it will complain about it not being a function.
        // We check the deterministic func condition immediately below.
        config.set(write_hook_config_t(ql::deterministic_func{ql::wire_func_t(v->as_func(env->env))},
                                       reql_version_t::LATEST));

    config_specified_with_value:

        config->func.det_func.compile_wire_func()->assert_deterministic(
                constant_now_t::no,
                "Write hook functions must be deterministic.");

        {
            optional<size_t> arity = config->func.det_func.compile_wire_func()->arity();

            rcheck(arity.has_value() && arity.get() == 3,
                   base_exc_t::LOGIC,
                   strprintf("Write hook functions must expect 3 arguments."));
        }

        deletion_message = true;

        // QQQ: In 2.4.x we didn't call get_write_hook inside of a try catch block for the permission_error_t exception that might get thrown.  What happens if the user doesn't have permission?

    config_specified_without_value:
        if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            if (!artificial_reql_cluster_interface_t::get_table_id(table.table_name).has_value()) {
                rfail_prov_table_dne(table);
            }

            admin_err_t error{strprintf("Database `%s` is special; you can't set a "
                      "write hook on the tables in it.",
                      artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }

        std::pair<bool, reqlfdb_config_version> existed;
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                config_info<std::pair<namespace_id_t, table_config_t>>
                    info = expect_retrieve_table(txn, table, env->env->interruptor);

                std::pair<bool, reqlfdb_config_version> old_existed = config_cache_set_write_hook(
                    txn, env->env->get_user_context(),
                    table, config, env->env->interruptor);
                commit(txn, env->env->interruptor);
                existed = old_existed;
            });
            guarantee_fdb_TODO(loop_err, "retry loop fail in get_write_hook_term");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        env->env->get_rdb_ctx()->config_caches.get()->note_version(existed.second);

        datum_string_t message = deletion_message ? datum_string_t("deleted") :
            existed.first ? datum_string_t("replaced") :
                datum_string_t("created");

        ql::datum_object_builder_t res;
        res.overwrite(message, datum_t(1.0));
        return new_val(std::move(res).to_datum());
    }

    const char *name() const override { return "set_write_hook"; }
};

class get_write_hook_term_t final : public op_term_t {
public:
    get_write_hook_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1)) { }

    deterministic_t is_deterministic() const override {
        return deterministic_t::no();
    }

    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const override {
        counted_t<table_t> table = args->arg(env, 0)->as_table(env->env);

        if (table->db->name == artificial_reql_cluster_interface_t::database_name) {
            return new_val(datum_t::null());
        }

        table_config_t table_config;
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                // TODO: Read-only txn.
                auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
                    = env->env->get_user_context().transaction_require_db_and_table_config_permission(
                        txn, table->db->id, table->get_id());

                table_config_t cfg = config_cache_get_table_config(txn,
                    table->tbl->cv.assert_nonempty(),
                    table->get_id(),
                    env->env->interruptor);

                auth_fut.block_and_check(env->env->interruptor);

                table_config = std::move(cfg);
            });
            guarantee_fdb_TODO(loop_err, "retry loop fali in get_write_hook_term");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
        datum_t write_hook = convert_write_hook_to_datum(table_config.write_hook);
        return new_val(std::move(write_hook));
    }

    const char *name() const override { return "get_write_hook"; }
};

counted_t<term_t> make_set_write_hook_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<set_write_hook_term_t>(env, term);
}
counted_t<term_t> make_get_write_hook_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<get_write_hook_term_t>(env, term);
}

} // namespace ql
