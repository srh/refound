// Copyright 2010-2014 RethinkDB, all rights reserved
#include "rdb_protocol/real_table.hpp"

#include "clustering/auth/permission_error.hpp"
#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "fdb/xtore.hpp"
#include "fdb/retry_loop.hpp"
#include "math.hpp"
#include "rdb_protocol/geo/ellipsoid.hpp"
#include "rdb_protocol/geo/distances.hpp"
#include "rdb_protocol/context.hpp"
#include "rdb_protocol/datum_stream.hpp"
#include "rdb_protocol/datum_stream/lazy.hpp"
#include "rdb_protocol/datum_stream/readers.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/math_utils.hpp"
#include "rdb_protocol/protocol.hpp"

namespace_id_t real_table_t::get_id() const {
    return uuid;
}

const std::string &real_table_t::get_pkey() const {
    return table_config->basic.primary_key;
}

// QQQ: Verify that these functions are called in a context that handles config_version_exc_t.

uint64_t real_table_t::read_count(ql::env_t *env, read_mode_t read_mode) const {
    read_t read(count_read_t{}, env->profile(), read_mode);
    read_response_t res;
    read_with_profile(env, read, &res);
    count_read_response_t *c_res = boost::get<count_read_response_t>(&res.response);
    r_sanity_check(c_res);
    return c_res->value;
}

ql::datum_t real_table_t::read_row(
    ql::env_t *env, ql::datum_t pval, read_mode_t read_mode) const {
    read_t read(point_read_t(store_key_t(pval.print_primary())),
                env->profile(), read_mode);
    read_response_t res;
    read_with_profile(env, read, &res);
    point_read_response_t *p_res = boost::get<point_read_response_t>(&res.response);
    r_sanity_check(p_res);
    return p_res->data;
}

// OOO: Fdb-ize the readers/datum stream stuff some of these functions use.

scoped_ptr_t<ql::reader_t> real_table_t::read_all_with_sindexes(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t,
        const std::string &table_name,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        read_mode_t read_mode) const {
    // This is a separate method because we need the sindex values from the rget_reader_t
    // in order to make the algorithm in eq_join work. The other method sometimes does
    // not fill in this information.

    if (datumspec.is_empty()) {
        return make_scoped<ql::empty_reader_t>(
            counted_t<const real_table_t>(this),
            table_name);
    }
    if (sindex == get_pkey()) {
        return make_scoped<ql::rget_reader_t>(
            counted_t<const real_table_t>(this),
            ql::primary_readgen_t::make(
                env, table_name, read_mode, datumspec, sorting));
    } else {
        return make_scoped<ql::rget_reader_t>(
	        counted_t<const real_table_t>(this),
                ql::sindex_readgen_t::make(
                    env,
                    table_name,
                    read_mode,
                    sindex,
                    datumspec,
                    sorting,
                    require_sindexes_t::YES));
    }
}

scoped<ql::datum_stream_t> real_table_t::read_all(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        read_mode_t read_mode) const {
    if (datumspec.is_empty()) {
        return make_scoped<ql::lazy_datum_stream_t>(
            make_scoped<ql::empty_reader_t>(
	        counted_t<const real_table_t>(this),
                table_name),
            bt);
    }
    if (sindex == get_pkey()) {
        return make_scoped<ql::lazy_datum_stream_t>(
            make_scoped<ql::rget_reader_t>(
		counted_t<const real_table_t>(this),
                ql::primary_readgen_t::make(
                    env, table_name, read_mode, datumspec, sorting)),
            bt);
    } else {
        return make_scoped<ql::lazy_datum_stream_t>(
            make_scoped<ql::rget_reader_t>(
	        counted_t<const real_table_t>(this),
                ql::sindex_readgen_t::make(
                    env, table_name, read_mode, sindex, datumspec, sorting)),
            bt);
    }
}

#if RDB_CF
counted_t<ql::datum_stream_t> real_table_t::read_changes(
        ql::env_t *env,
        const ql::changefeed::streamspec_t &ss,
        ql::backtrace_id_t bt) const {
    return changefeed_client->new_stream(env, ss, uuid, bt);
}
#endif

scoped<ql::datum_stream_t> real_table_t::read_intersecting(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name,
        read_mode_t read_mode,
        const ql::datum_t &query_geometry) const {

    return make_scoped<ql::lazy_datum_stream_t>(
        make_scoped<ql::intersecting_reader_t>(
            counted_t<const real_table_t>(this),
            ql::intersecting_readgen_t::make(
                env, table_name, read_mode, sindex, query_geometry)),
        bt);
}

ql::datum_t real_table_t::read_nearest(
        ql::env_t *env,
        const std::string &sindex,
        const std::string &table_name,
        read_mode_t read_mode,
        lon_lat_point_t center,
        double max_dist,
        uint64_t max_results,
        const ellipsoid_spec_t &geo_system,
        dist_unit_t dist_unit,
        const ql::configured_limits_t &limits) const {

    nearest_geo_read_t geo_read(
        center,
        max_dist,
        max_results,
        geo_system,
        table_name,
        sindex,
        env->get_serializable_env());
    read_t read(geo_read, env->profile(), read_mode);
    read_response_t res;
    try {
        res = apply_read(
            env->get_rdb_ctx()->fdb, env->get_rdb_ctx(), cv.assert_nonempty(),
            env->get_user_context(),
            uuid, *table_config,
            read,
            env->interruptor);
    } catch (const cannot_perform_query_exc_t &ex) {
        rfail_datum(ql::base_exc_t::OP_FAILED, "Cannot perform read: %s", ex.what());
    } catch (auth::permission_error_t const &error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", error.what());
    } catch (const config_version_exc_t &ex) {
        rfail_datum(ql::base_exc_t::OP_FAILED, "System configuration changed while performing read");
    }

    nearest_geo_read_response_t *g_res =
        boost::get<nearest_geo_read_response_t>(&res.response);
    r_sanity_check(g_res);

    ql::exc_t *error = boost::get<ql::exc_t>(&g_res->results_or_error);
    if (error != NULL) {
        throw *error;
    }

    auto *result =
        boost::get<nearest_geo_read_response_t::result_t>(&g_res->results_or_error);
    guarantee(result != NULL);

    // Generate the final output, converting distance units on the way.
    ql::datum_array_builder_t formatted_result(limits);
    for (size_t i = 0; i < result->size(); ++i) {
        ql::datum_object_builder_t one_result;
        const double converted_dist =
            convert_dist_unit((*result)[i].first, dist_unit_t::M, dist_unit);
        bool dup;
        dup = one_result.add("dist", ql::datum_t(converted_dist));
        r_sanity_check(!dup);
        dup = one_result.add("doc", (*result)[i].second);
        r_sanity_check(!dup);
        formatted_result.add(std::move(one_result).to_datum());
    }
    return std::move(formatted_result).to_datum();
}

ql::datum_t real_table_t::write_batched_replace(
    ql::env_t *env,
    const std::vector<ql::datum_t> &keys,
    const ql::deterministic_func &func,
    return_changes_t return_changes,
    durability_requirement_t durability,
    ignore_write_hook_t ignore_write_hook) const {

    std::vector<store_key_t> store_keys;
    store_keys.reserve(keys.size());
    for (auto it = keys.begin(); it != keys.end(); it++) {
        store_keys.push_back(store_key_t((*it).print_primary()));
    }

    ql::datum_t stats((std::map<datum_string_t, ql::datum_t>()));
    std::set<std::string> conditions;

    // TODO: Does this really need a pkey field?
    // QQQ: Given an out of date table config, performing computations on the documents with the pkey before we've done check_cv is wrong.  (But we did eval r.table here so this is actually a legit calculation.  Revisit what we do if cv is out of date, downstream from this.  Don't we have a non-provisional table id?  So it's just table config that's out of date (for sindex updates and such).)
    batched_replace_t write(
        std::move(store_keys),
        get_pkey(),
        func,
        ignore_write_hook,
        env->get_serializable_env(),
        return_changes);
    write_t w(std::move(write), durability, env->profile(), env->limits());
    write_response_t response;
    write_with_profile(env, &w, &response);
    auto dp = boost::get<ql::datum_t>(&response.response);
    r_sanity_check(dp != NULL);
    stats = stats.merge(*dp, ql::stats_merge, env->limits(), &conditions);

    ql::datum_object_builder_t result(stats);
    result.add_warnings(conditions, env->limits());
    return std::move(result).to_datum();
}

ql::datum_t real_table_t::write_batched_insert(
    ql::env_t *env,
    std::vector<ql::datum_t> &&inserts,
    UNUSED std::vector<bool> &&pkey_is_autogenerated,
    conflict_behavior_t conflict_behavior,
    optional<ql::deterministic_func> conflict_func,
    return_changes_t return_changes,
    durability_requirement_t durability,
    ignore_write_hook_t ignore_write_hook) const {

    ql::datum_t stats((std::map<datum_string_t, ql::datum_t>()));
    std::set<std::string> conditions;

    // TODO: Does this really need a pkey field?
    batched_insert_t write(
        std::move(inserts),
        get_pkey(),
        ignore_write_hook,
        conflict_behavior,
        conflict_func,
        env->limits(),
        env->get_serializable_env(),
        return_changes);
    write_t w(std::move(write), durability, env->profile(), env->limits());
    write_response_t response;
    write_with_profile(env, &w, &response);
    auto dp = boost::get<ql::datum_t>(&response.response);
    r_sanity_check(dp != NULL);
    stats = stats.merge(*dp, ql::stats_merge, env->limits(), &conditions);

    ql::datum_object_builder_t result(stats);
    result.add_warnings(conditions, env->limits());
    return std::move(result).to_datum();
}

void real_table_t::read_with_profile(ql::env_t *env, const read_t &read,
        read_response_t *response) const {
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
    try {
        *response = apply_read(
            env->get_rdb_ctx()->fdb,
            env->get_rdb_ctx(),
            cv.assert_nonempty(),
            env->get_user_context(),
            uuid,
            *table_config,
            read,
            env->interruptor);
    } catch (const cannot_perform_query_exc_t &e) {
        rfail_datum(ql::base_exc_t::OP_FAILED, "Cannot perform read: %s", e.what());
    } catch (auth::permission_error_t const &error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", error.what());
    } catch (const config_version_exc_t &ex) {
        rfail_datum(ql::base_exc_t::OP_FAILED, "System configuration changed while performing read");
    }

    /* Append the results of the profile to the current task */
    splitter.give_splits(1, response->event_log);
}

void real_table_t::write_with_profile(ql::env_t *env, write_t *write,
        write_response_t *response) const {
    PROFILE_STARTER_IF_ENABLED(
        env->profile() == profile_bool_t::PROFILE, "Perform write", env->trace);
    // TODO: Remove splitter.
    profile::splitter_t splitter(env->trace);
    /* propagate whether or not we're doing profiles */
    // TODO: What is up with this write->profile assignment?  What about reads?
    write->profile = env->profile();

    /* Do the actual write. */
    try {
        *response = apply_write(
            env->get_rdb_ctx()->fdb,
            cv.assert_nonempty(),
            env->get_user_context(),
            uuid,
            *table_config,
            *write,
            env->interruptor);
    } catch (const cannot_perform_query_exc_t &e) {
        ql::base_exc_t::type_t type;
        // QQQ: Is INDETERMINATE possible?  Only when txn commit fails indeterminately.  Make guarantee_fdb_TODO hook up indeterminate case with OP_INDETERMINATE.
        switch (e.get_query_state()) {
        case query_state_t::FAILED:
            type = ql::base_exc_t::OP_FAILED;
            break;
        case query_state_t::INDETERMINATE:
            type = ql::base_exc_t::OP_INDETERMINATE;
            break;
        default: unreachable();
        }
        rfail_datum(type, "Cannot perform write: %s", e.what());
    } catch (auth::permission_error_t const &error) {
        rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", error.what());
    } catch (const config_version_exc_t &ex) {
        rfail_datum(ql::base_exc_t::OP_FAILED, "System configuration changed while performing write");
    }

    /* Append the results of the profile to the current task */
    splitter.give_splits(1, response->event_log);
}

