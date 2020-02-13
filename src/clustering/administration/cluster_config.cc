// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/cluster_config.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/tables/name_resolver.hpp"
#include "containers/lifetime.hpp"

cluster_config_artificial_table_backend_t::cluster_config_artificial_table_backend_t(
        RDB_CF_UNUSED lifetime_t<name_resolver_t const &> name_resolver)
#if RDB_CF
    : caching_cfeed_artificial_table_backend_t(
        name_string_t::guarantee_valid("cluster_config"), name_resolver)
#else
    : artificial_table_backend_t(
        name_string_t::guarantee_valid("cluster_config"))
#endif
{
}

cluster_config_artificial_table_backend_t::~cluster_config_artificial_table_backend_t() {
#if RDB_CF
    begin_changefeed_destruction();
#endif
}

std::string cluster_config_artificial_table_backend_t::get_primary_key_name() {
    return "id";
}

bool cluster_config_artificial_table_backend_t::read_all_rows_as_vector(
        UNUSED auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out) {
    rows_out->clear();

    for (auto it = docs.begin(); it != docs.end(); ++it) {
        ql::datum_t row;
        if (!it->second->read(interruptor, &row, error_out)) {
            return false;
        }
        rows_out->push_back(row);
    }
    return true;
}

bool cluster_config_artificial_table_backend_t::read_row(
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        admin_err_t *error_out) {
    if (primary_key.get_type() != ql::datum_t::R_STR) {
        *row_out = ql::datum_t();
        return true;
    }
    auto it = docs.find(primary_key.as_str().to_std());
    if (it == docs.end()) {
        *row_out = ql::datum_t();
        return true;
    }
    return it->second->read(interruptor, row_out, error_out);
}

bool cluster_config_artificial_table_backend_t::write_row(
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        UNUSED bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor,
        admin_err_t *error_out) {
    if (!new_value_inout->has()) {
        *error_out = admin_err_t{
            "It's illegal to delete rows from the `rethinkdb.cluster_config` table.",
            query_state_t::FAILED};
        return false;
    }
    const char *missing_message = "It's illegal to insert new rows into the "
        "`rethinkdb.cluster_config` table.";
    if (primary_key.get_type() != ql::datum_t::R_STR) {
        *error_out = admin_err_t{missing_message, query_state_t::FAILED};
        return false;
    }
    auto it = docs.find(primary_key.as_str().to_std());
    if (it == docs.end()) {
        *error_out = admin_err_t{missing_message, query_state_t::FAILED};
        return false;
    }
    return it->second->write(interruptor, new_value_inout, error_out);
}

void cluster_config_artificial_table_backend_t::set_notifications(RDB_CF_UNUSED bool should_notify) {
#if RDB_CF
    /* Note that we aren't actually modifying the `docs` map itself, just the objects
    that it points at. So this could have been `const auto &pair`, but that might be
    misleading. */
    for (auto &&pair : docs) {
        if (should_notify) {
            std::string name = pair.first;
            pair.second->set_notification_callback(
                [this, name]() {
                    notify_row(ql::datum_t(datum_string_t(name)));
                });
        } else {
            pair.second->set_notification_callback(nullptr);
        }
    }
#endif
}
