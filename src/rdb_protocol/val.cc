// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/val.hpp"

#include "containers/name_string.hpp"
#include "debug.hpp"
#include "rdb_protocol/datum_stream/readers.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/math_utils.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/term.hpp"
#include "rdb_protocol/types.hpp"
#include "stl_utils.hpp"
#include "thread_local.hpp"

namespace ql {

// TODO: Remove, eventually.
scoped<table_t> provisional_to_table(
        env_t *env, const provisional_table_id &prov_table) {
    return provisional_to_table(
        env->get_rdb_ctx()->fdb,
        env->interruptor,
        env->get_rdb_ctx()->config_caches.get(),
        env->get_rdb_ctx()->artificial_interface_or_null,
        prov_table);
}

selection_t::selection_t(counted_t<table_t> _table, scoped<datum_stream_t> &&_seq)
        : table(std::move(_table)), seq(std::move(_seq)) { }
selection_t::~selection_t() {}

class provisional_get_selection final : public single_selection_t {
public:
    provisional_get_selection(provisional_table_id &&_prov_table,
            datum_t _key)
        : prov_table(std::move(_prov_table)),
          table(),
          key(std::move(_key)),
          row() { }
    datum_t get(env_t *env) override {
        if (!row.has()) {
            std::pair<datum_t, scoped<table_t>> tmp = prov_read_row(env, prov_table, key);
            row = std::move(tmp.first);
            table = std::move(tmp.second);
        }
        return row;
    }
    datum_t replace(
            env_t *env,
            counted_t<const func_t> f,
            bool nondet_ok,
            durability_requirement_t dur_req,
            return_changes_t return_changes,
            ignore_write_hook_t ignore_write_hook) && override {
        counted_t<table_t> &tbl = get_tbl(env);
        std::vector<datum_t> keys = {key};
        // We don't need to fetch the value for deterministic replacements.
        std::vector<datum_t> vals = {
            f->is_deterministic().test(single_server_t::no, constant_now_t::yes) ? datum_t() : get(env)};
        return tbl->batched_replace(
            env, vals, keys, f, nondet_ok, dur_req, return_changes, ignore_write_hook);
    }
#if RDB_CF
    changefeed::keyspec_t::spec_t get_spec() const final {
        return changefeed::keyspec_t::point_t{key};
    }
#endif
    counted_t<table_t> &get_tbl(env_t *env) final override {
        if (!table.has()) {
            table = provisional_to_table(env, prov_table);
        }
        return table;
    }
private:
    provisional_table_id prov_table;
    counted_t<table_t> table;
    datum_t key, row;
};

class get_selection_t final : public single_selection_t {
public:
    get_selection_t(counted_t<table_t> _tbl,
                    datum_t _key,
                    datum_t _row = datum_t())
        : tbl(std::move(_tbl)),
          key(std::move(_key)),
          row(std::move(_row)) { }
    datum_t get(env_t *env) override {
        if (!row.has()) {
            row = tbl->get_row(env, key);
        }
        return row;
    }
    datum_t replace(
        env_t *env,
        counted_t<const func_t> f,
        bool nondet_ok,
        durability_requirement_t dur_req,
        return_changes_t return_changes,
        ignore_write_hook_t ignore_write_hook) && override {
        std::vector<datum_t> keys = {key};
        // We don't need to fetch the value for deterministic replacements.
        std::vector<datum_t> vals = {
            f->is_deterministic().test(single_server_t::no, constant_now_t::yes) ? datum_t() : get(env)};
        return tbl->batched_replace(
            env, vals, keys, f, nondet_ok, dur_req, return_changes, ignore_write_hook);
    }
#if RDB_CF
    changefeed::keyspec_t::spec_t get_spec() const final {
        return changefeed::keyspec_t::point_t{key};
    }
#endif
    counted_t<table_t> &get_tbl(env_t *) final override { return tbl; }
private:
    counted_t<table_t> tbl;
    datum_t key, row;
};

class extreme_selection_t final : public single_selection_t {
public:
    extreme_selection_t(backtrace_id_t _bt,
                        scoped<table_slice_t> &&_slice,
                        std::string _err)
        : bt(std::move(_bt)),
          slice(std::move(_slice)),
          err(std::move(_err)) { }
    datum_t get(env_t *env) override {
        if (!row.has()) {
            batchspec_t batchspec = batchspec_t::all().with_at_most(1);
            r_sanity_check(slice.has());
            row = std::move(*slice).as_seq(env, bt)->next(env, batchspec);
            slice.reset();
            if (!row.has()) {
                rfail_src(bt, base_exc_t::LOGIC, "%s", err.c_str());
            }
        }
        return row;
    }
    datum_t replace(
        env_t *env,
        counted_t<const func_t> f,
        bool nondet_ok,
        durability_requirement_t dur_req,
        return_changes_t return_changes,
        ignore_write_hook_t ignore_write_hook) && override {
        counted_t<table_t> table = slice->get_tbl();
        std::vector<datum_t > vals{get(env)};
        std::vector<datum_t > keys{
            vals[0].get_field(
                datum_string_t(table->get_pkey()),
                NOTHROW)};
        r_sanity_check(keys[0].has());
        return table->batched_replace(
            env, vals, keys, f, nondet_ok, dur_req, return_changes, ignore_write_hook);
    }
#if RDB_CF
    changefeed::keyspec_t::spec_t get_spec() const final {
        return ql::changefeed::keyspec_t::limit_t{slice->get_range_spec(), 1};
    }
#endif  // RDB_CF
    // Must never be used after get().  Just to maintain some linearity condition, for now.
    const counted_t<table_t> &get_tbl(env_t *) final override { return slice->get_tbl(); }
private:
    backtrace_id_t bt;
    scoped<table_slice_t> slice;
    datum_t row;
    std::string err;
};

scoped<single_selection_t> single_selection_t::provisionally_from_key(
        provisional_table_id &&table, datum_t key) {
    return make_scoped<provisional_get_selection>(
        std::move(table), std::move(key));
}
scoped<single_selection_t> single_selection_t::from_key(
    counted_t<table_t> table, datum_t key) {
    return make_scoped<get_selection_t>(
        std::move(table), std::move(key));
}
scoped<single_selection_t> single_selection_t::from_row(
    counted_t<table_t> table, datum_t row) {
    datum_t d = row.get_field(datum_string_t(table->get_pkey()), NOTHROW);
    r_sanity_check(d.has());
    return make_scoped<get_selection_t>(
        std::move(table), std::move(d), std::move(row));
}
scoped<single_selection_t> single_selection_t::from_slice(
    backtrace_id_t bt,
    scoped<table_slice_t> &&table, std::string err) {
    return make_scoped<extreme_selection_t>(
        std::move(bt), std::move(table), std::move(err));
}

table_slice_t::table_slice_t(counted_t<table_t> _tbl,
                             optional<std::string> _idx,
                             sorting_t _sorting,
                             datum_range_t _bounds)
    : bt_rcheckable_t(_tbl->backtrace()),
      tbl(std::move(_tbl)), idx(std::move(_idx)),
      sorting(_sorting), bounds(std::move(_bounds)) { }


scoped<datum_stream_t> table_slice_t::as_seq(
    env_t *env, backtrace_id_t _bt) && {
    // Empty bounds will be handled by as_seq with empty_reader_t
    return tbl->as_seq(env, idx.has_value() ? *idx : tbl->get_pkey(), _bt, bounds, sorting);
}

scoped<datum_stream_t> table_slice_t::as_seq_with_sorting(
        std::string _idx, sorting_t _sorting, env_t *env, backtrace_id_t bt) && {
    // Caller has to meet some particular requirements.
    r_sanity_check(!idx.has_value() || *idx == _idx);
    r_sanity_check(sorting == sorting_t::UNORDERED);
    return tbl->as_seq(env, _idx, bt, bounds, _sorting);
}

scoped<table_slice_t>
table_slice_t::with_sorting(std::string _idx, sorting_t _sorting) && {
    rcheck(sorting == sorting_t::UNORDERED, base_exc_t::LOGIC,
           "Cannot perform multiple indexed ORDER_BYs on the same table.");
    bool idx_legal = idx.has_value() ? (*idx == _idx) : true;
    r_sanity_check(idx_legal || !bounds.is_universe());
    rcheck(idx_legal, base_exc_t::LOGIC,
           strprintf("Cannot order by index `%s` after calling BETWEEN on index `%s`.",
                     _idx.c_str(), (*idx).c_str()));
    return make_scoped<table_slice_t>(tbl, make_optional(std::move(_idx)), _sorting, bounds);
}

scoped<table_slice_t>
table_slice_t::with_bounds(std::string _idx, datum_range_t _bounds) && {
    rcheck(bounds.is_universe(), base_exc_t::LOGIC,
           "Cannot perform multiple BETWEENs on the same table.");
    bool idx_legal = idx.has_value() ? (*idx == _idx) : true;
    r_sanity_check(idx_legal || sorting != sorting_t::UNORDERED);
    rcheck(idx_legal, base_exc_t::LOGIC,
           strprintf("Cannot call BETWEEN on index `%s` after ordering on index `%s`.",
                     _idx.c_str(), (*idx).c_str()));
    return make_scoped<table_slice_t>(
        tbl, make_optional(std::move(_idx)), sorting, std::move(_bounds));
}

#if RDB_CF
ql::changefeed::keyspec_t::range_t table_slice_t::get_range_spec() {
    return ql::changefeed::keyspec_t::range_t{
        std::vector<transform_variant_t>(),
        idx && *idx == tbl->get_pkey() ? r_nullopt : idx,
        sorting,
        datumspec_t(bounds),
        r_nullopt};
}
#endif  // RDB_CF

scoped<datum_stream_t> table_t::as_seq(
    env_t *env,
    const std::string &idx,
    backtrace_id_t _bt,
    const datum_range_t &bounds,
    sorting_t sorting) {
    return tbl->read_all(
        env,
        idx,
        _bt,
        display_name(),
        datumspec_t(bounds),
        sorting,
        read_mode);
}

table_t::table_t(counted_t<const base_table_t> &&_tbl,
                 counted_t<const db_t> _db, const name_string_t &_name,
                 read_mode_t _read_mode, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      db(_db),
      name(_name),
      tbl(std::move(_tbl)),
      read_mode(_read_mode)
{ }

datum_t table_t::make_error_datum(const base_exc_t &exception) {
    datum_object_builder_t d;
    d.add_error(exception.what());
    return std::move(d).to_datum();
}

ql::datum_t clean_errors(ql::datum_t reply) {
    ql::datum_t array = reply.get_field("changes");
    ql::datum_array_builder_t clean_array(ql::configured_limits_t::unlimited);
    for (size_t i = 0; i < array.arr_size(); ++i) {
        ql::datum_t updated = array.get(i);
        if (updated.get_field("error", NOTHROW).has()) {
            clean_array.add(ql::datum_t{
                    std::map<datum_string_t, datum_t>{
                        std::pair<datum_string_t, datum_t> {
                            datum_string_t("old_val"),
                                updated.get_field("old_val")},
                            std::pair<datum_string_t, datum_t> {
                                datum_string_t("new_val"),
                                    updated.get_field("new_val")},
                                std::pair<datum_string_t, datum_t> {
                                    datum_string_t("error"),
                                        updated.get_field("error")}}});
        } else {
            clean_array.add(ql::datum_t{
                    std::map<datum_string_t, datum_t>{
                        std::pair<datum_string_t, datum_t> {
                            datum_string_t("old_val"),
                                updated.get_field("old_val")},
                            std::pair<datum_string_t, datum_t> {
                                datum_string_t("new_val"),
                                    updated.get_field("new_val")}}});
        }
    }

    reply = reply.merge(
        datum_t{std::map<datum_string_t, datum_t>{
                std::pair<datum_string_t, datum_t>{
                    datum_string_t{"changes"},
                        std::move(clean_array).to_datum()}}});
    return reply;
}

datum_t table_t::batched_replace(
    env_t *env,
    const std::vector<datum_t> &vals,
    const std::vector<datum_t> &keys,
    counted_t<const func_t> replacement_generator,
    bool nondeterministic_replacements_ok,
    durability_requirement_t durability_requirement,
    return_changes_t return_changes,
    ignore_write_hook_t ignore_write_hook) {
    r_sanity_check(vals.size() == keys.size());

    if (vals.empty()) {
        /* Maybe we ought to require write permission and (if ignore_write_hook is YES)
        config permission on the table even in the case with no documents.  But we
        didn't before, so let's not touch that. */
        return ql::datum_t::empty_object();
    }

    if (!replacement_generator->is_deterministic().test(single_server_t::no,
                                                        constant_now_t::yes)) {
        r_sanity_check(nondeterministic_replacements_ok);
        datum_object_builder_t stats;
        std::vector<datum_t> replacement_values;
        replacement_values.reserve(vals.size());
        for (size_t i = 0; i < vals.size(); ++i) {
            r_sanity_check(vals[i].has());
            datum_t new_val;
            try {
                new_val = replacement_generator->call(env, vals[i])->as_datum(env);
                new_val.rcheck_valid_replace(vals[i], keys[i],
                                             datum_string_t(get_pkey()));
                r_sanity_check(new_val.has());
                replacement_values.push_back(new_val);
            } catch (const base_exc_t &e) {
                stats.add_error(e.what());
            }
        }
        std::vector<bool> pkey_was_autogenerated(vals.size(), false);
        datum_t insert_stats = batched_insert(
            env,
            std::move(replacement_values),
            std::move(pkey_was_autogenerated),
            conflict_behavior_t::REPLACE,
            r_nullopt,
            durability_requirement,
            return_changes,
            ignore_write_hook);
        std::set<std::string> conditions;
        datum_t merged
            = std::move(stats).to_datum().merge(insert_stats, stats_merge,
                                                 env->limits(), &conditions);
        datum_object_builder_t result(merged);
        result.add_warnings(conditions, env->limits());
        if (return_changes == return_changes_t::ALWAYS) {
            return clean_errors(std::move(result).to_datum());
        } else {
            return std::move(result).to_datum();
        }
    } else {
        if (return_changes == return_changes_t::ALWAYS) {
            return clean_errors(tbl->write_batched_replace(
                env, keys, deterministic_func{wire_func_t{replacement_generator}}, return_changes,
                durability_requirement, ignore_write_hook));
        } else {
            return tbl->write_batched_replace(
                env, keys, deterministic_func{wire_func_t{replacement_generator}}, return_changes,
                durability_requirement, ignore_write_hook);
        }
    }
}

datum_t trivial_error_datum(std::string msg) {
    return datum_t{std::map<datum_string_t, datum_t>{
            std::pair<datum_string_t, datum_t> {
                datum_string_t("old_val"),
                datum_t::null()
            },
            std::pair<datum_string_t, datum_t> {
                datum_string_t("new_val"),
                datum_t::null()
            },
            std::pair<datum_string_t, datum_t> {
                datum_string_t("error"),
                datum_t(datum_string_t(msg))
            }
        }};
}

datum_t table_t::batched_insert(
    env_t *env,
    std::vector<datum_t> &&insert_datums,
    std::vector<bool> &&pkey_was_autogenerated,
    conflict_behavior_t conflict_behavior,
    optional<ql::deterministic_func> conflict_func,
    durability_requirement_t durability_requirement,
    return_changes_t return_changes,
    ignore_write_hook_t ignore_write_hook) {

    datum_object_builder_t stats;
    std::vector<datum_t> valid_inserts;
    std::vector<bool> valid_pkey_was_autogenerated;
    std::vector<datum_t> insert_keys;
    std::deque<std::string> trivial_errors;

    valid_inserts.reserve(insert_datums.size());
    insert_keys.reserve(insert_datums.size());
    for (auto it = insert_datums.begin(); it != insert_datums.end(); ++it) {
        try {
            datum_string_t pkey_w(get_pkey());
            it->rcheck_valid_replace(datum_t(),
                                     datum_t(),
                                     pkey_w);
            const ql::datum_t &keyval = (*it).get_field(pkey_w);
            keyval.print_primary(); // does error checking
            valid_inserts.push_back(std::move(*it));
            valid_pkey_was_autogenerated.push_back(
                std::move(pkey_was_autogenerated[it-insert_datums.begin()]));
            insert_keys.push_back(keyval);
        } catch (const base_exc_t &e) {
            // Each of these trivial errors should give a
            // {old_val: null, new_val: null} change if we're returning changes.
            // These have to get put in in order, so we handle them below.
            insert_keys.push_back(datum_t());
            trivial_errors.push_back(e.what());
            stats.add_error(e.what());
        }
    }

    ql::datum_array_builder_t new_changes(env->limits());
    std::multimap<datum_t, datum_t> pkey_to_change;
    datum_t insert_stats;

    if (!valid_inserts.empty()) {
        // Do actual insert.
        insert_stats =
            tbl->write_batched_insert(
                env,
                std::move(valid_inserts),
                std::move(valid_pkey_was_autogenerated),
                conflict_behavior,
                conflict_func,
                return_changes,
                durability_requirement,
                ignore_write_hook);

        if (return_changes != return_changes_t::NO) {
            // Generate map to order changes
            ql::datum_t changes = insert_stats.get_field("changes");
            for (size_t i = 0; i < changes.arr_size(); ++i) {
                ql::datum_t pkey;
                if (changes.get(i).get_field("error", NOTHROW).has()) {
                    // There was an error that prevented the insert
                    pkey = changes.get(i)
                        .get_field("fake_new_val")
                        .get_field(get_pkey().c_str(), NOTHROW);
                } else if (changes.get(i)
                    .get_field("new_val")
                    .get_type() == datum_t::R_NULL) {
                    // We're deleting using a conflict resolution function in insert
                    pkey = changes.get(i)
                        .get_field("old_val")
                        .get_field(get_pkey().c_str(), NOTHROW);
                } else {
                    pkey = changes.get(i)
                        .get_field("new_val")
                        .get_field(get_pkey().c_str(), NOTHROW);
                }
                pkey_to_change.insert(std::pair<datum_t, datum_t>{pkey, changes.get(i)});
            }

            for (const auto &inserted_key : insert_keys) {
                if (inserted_key.has()) {
                    auto updated_iterator = pkey_to_change.equal_range(inserted_key);
                    auto updated = updated_iterator.first;
                    if (updated != pkey_to_change.end()) {
                        new_changes.add(std::move(updated->second));
                        pkey_to_change.erase(updated);
                    }
                } else {
                    r_sanity_check(trivial_errors.size() > 0);
                    new_changes.add(
                        trivial_error_datum(
                            trivial_errors.front()));
                    trivial_errors.pop_front();
                }
            }
        }
    } else if (!insert_datums.empty()) {
        // Handle the trivial errors from above.
        r_sanity_check(trivial_errors.size() > 0);
        new_changes.add(
            trivial_error_datum(
                trivial_errors.front()));
        trivial_errors.pop_front();
    }

    if (return_changes != return_changes_t::NO) {
        insert_stats = insert_stats.merge(
            datum_t{std::map<datum_string_t, datum_t>{
                    std::pair<datum_string_t, datum_t>{
                        datum_string_t{"changes"},
                            std::move(new_changes).to_datum()}}});
    }

    std::set<std::string> conditions;
    datum_t merged = std::move(stats).to_datum();

    if (insert_stats.has()) {
        merged = merged.merge(insert_stats, stats_merge,
                          env->limits(), &conditions);
    }

    datum_object_builder_t result(merged);
    result.add_warnings(conditions, env->limits());
    if (return_changes == return_changes_t::ALWAYS) {
        return clean_errors(std::move(result).to_datum());
    } else {
        return std::move(result).to_datum();
    }
}

namespace_id_t table_t::get_id() const {
    return tbl->get_id();
}

const std::string &table_t::get_pkey() const {
    return tbl->get_pkey();
}

datum_t table_t::get_row(env_t *env, datum_t pval) {
    return tbl->read_row(env, pval, read_mode);
}

uint64_t table_t::get_count(env_t *env) {
    return tbl->read_count(env, read_mode);
}

scoped_ptr_t<reader_t> table_t::get_all_with_sindexes(
        env_t *env,
        const datumspec_t &datumspec,
        const std::string &get_all_sindex_id,
        backtrace_id_t _bt) {
    return tbl->read_all_with_sindexes(
        env,
        get_all_sindex_id,
        _bt,
        display_name(),
        datumspec,
        sorting_t::UNORDERED,
        read_mode);
}

scoped<datum_stream_t> table_t::get_all(
        env_t *env,
        const datumspec_t &datumspec,
        const std::string &get_all_sindex_id,
        backtrace_id_t _bt) {
    return tbl->read_all(
        env,
        get_all_sindex_id,
        _bt,
        display_name(),
        datumspec,
        sorting_t::UNORDERED,
        read_mode);
}

scoped<datum_stream_t> table_t::get_intersecting(
        env_t *env,
        const datum_t &query_geometry,
        const std::string &new_sindex_id,
        const bt_rcheckable_t *parent) {
    return tbl->read_intersecting(
        env,
        new_sindex_id,
        parent->backtrace(),
        display_name(),
        read_mode,
        query_geometry);
}

datum_t table_t::get_nearest(
        env_t *env,
        lon_lat_point_t center,
        double max_dist,
        uint64_t max_results,
        const ellipsoid_spec_t &geo_system,
        dist_unit_t dist_unit,
        const std::string &new_sindex_id,
        const configured_limits_t &limits) {
    return tbl->read_nearest(
        env,
        new_sindex_id,
        display_name(),
        read_mode,
        center,
        max_dist,
        max_results,
        geo_system,
        dist_unit,
        limits);
}

val_t::type_t::type_t(val_t::type_t::raw_type_t _raw_type) : raw_type(_raw_type) { }

// NOTE: This *MUST* be kept in sync with the surrounding code (not that it
// should have to change very often).
bool raw_type_is_convertible(val_t::type_t::raw_type_t _t1,
                             val_t::type_t::raw_type_t _t2) {
    const int t1 = _t1, t2 = _t2,
        DB               = val_t::type_t::DB,
        TABLE            = val_t::type_t::TABLE,
        TABLE_SLICE      = val_t::type_t::TABLE_SLICE,
        SELECTION        = val_t::type_t::SELECTION,
        SEQUENCE         = val_t::type_t::SEQUENCE,
        SINGLE_SELECTION = val_t::type_t::SINGLE_SELECTION,
        DATUM            = val_t::type_t::DATUM,
        FUNC             = val_t::type_t::FUNC,
        GROUPED_DATA     = val_t::type_t::GROUPED_DATA;
    switch (t1) {
    case DB:               return t2 == DB;
    case TABLE:            return t2 == TABLE || t2 == TABLE_SLICE
                                  || t2 == SELECTION || t2 == SEQUENCE;
    case TABLE_SLICE:      return t2 == TABLE_SLICE || t2 == SELECTION || t2 == SEQUENCE;
    case SELECTION:        return t2 == SELECTION || t2 == SEQUENCE;
    case SEQUENCE:         return t2 == SEQUENCE;
    case SINGLE_SELECTION: return t2 == SINGLE_SELECTION || t2 == DATUM;
    case DATUM:            return t2 == DATUM || t2 == SEQUENCE;
    case FUNC:             return t2 == FUNC;
    case GROUPED_DATA:     return t2 == GROUPED_DATA;
    default: unreachable();
    }
}
bool val_t::type_t::is_convertible(type_t rhs) const {
    return raw_type_is_convertible(raw_type, rhs.raw_type);
}

const char *val_t::type_t::name() const {
    switch (raw_type) {
    case DB: return "DATABASE";
    case TABLE: return "TABLE";
    case TABLE_SLICE: return "TABLE_SLICE";
    case SELECTION: return "SELECTION";
    case SEQUENCE: return "SEQUENCE";
    case SINGLE_SELECTION: return "SINGLE_SELECTION";
    case DATUM: return "DATUM";
    case FUNC: return "FUNCTION";
    case GROUPED_DATA: return "GROUPED_DATA";
    default: unreachable();
    }
}

val_t::val_t(datum_t _datum, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::DATUM),
      u(_datum) {
    guarantee(datum().has());
}

val_t::val_t(const counted_t<grouped_data_t> &groups,
             backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::GROUPED_DATA),
      u(groups) {
    guarantee(groups.has());
}

val_t::val_t(scoped<single_selection_t> &&_selection, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::SINGLE_SELECTION),
      u(std::move(_selection)) {
    guarantee(single_selection().has());
}

val_t::val_t(env_t *env, scoped<datum_stream_t> &&_sequence,
             backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::SEQUENCE),
      u(std::move(_sequence)) {
    guarantee(sequence().has());
    // Some streams are really arrays in disguise.
    datum_t arr = sequence()->as_array(env);
    if (arr.has()) {
        type = type_t::DATUM;
        u = std::move(arr);
    }
}

val_t::val_t(scoped<selection_t> &&_selection, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::SELECTION),
      u(std::move(_selection)) {
    guarantee(selection().has());
}

val_t::val_t(provisional_table_id &&_table, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::TABLE),
      u(std::move(_table)) {
}
val_t::val_t(scoped<table_slice_t> &&_slice, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::TABLE_SLICE),
      u(std::move(_slice)) {
    guarantee(table_slice().has());
}
val_t::val_t(provisional_db_id &&_db, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::DB),
      u(std::move(_db)) {
}
val_t::val_t(scoped<func_t> &&_func, backtrace_id_t _bt)
    : bt_rcheckable_t(_bt),
      type(type_t::FUNC),
      u(scoped<const func_t>(std::move(_func))) {
    guarantee(func().has());
}

val_t::~val_t() { }

val_t::type_t val_t::get_type() const { return type; }
const char * val_t::get_type_name() const { return get_type().name(); }

datum_t val_t::as_datum(env_t *env) const {
    if (type.raw_type == type_t::DATUM) {
        return datum();
    } else if (type.raw_type == type_t::SINGLE_SELECTION) {
        return single_selection()->get(env);
    }
    rcheck_literal_type(env, type_t::DATUM);
    unreachable();
}

provisional_table_id val_t::as_prov_table(env_t *env) && {
    rcheck_literal_type(env, type_t::TABLE);
    return std::move(table());
}

scoped<table_t> val_t::as_table(env_t *env) && {
    provisional_table_id prov_table = std::move(*this).as_prov_table(env);
    return provisional_to_table(env, prov_table);
}
scoped<table_slice_t> val_t::as_table_slice(env_t *env) && {
    // OOO: Make table_slice_t hold a provisional_table_id, ofc.  (Oh rry?)
    if (type.raw_type == type_t::TABLE) {
        return make_scoped<table_slice_t>(std::move(*this).as_table(env));
    } else {
        rcheck_literal_type(env, type_t::TABLE_SLICE);
        return std::move(table_slice());
    }
}

// Equivalent to as_seq(env)->is_grouped().
bool val_t::is_grouped_seq() const {
    if (type.raw_type == type_t::SEQUENCE) {
        return sequence()->is_grouped();
    } else if (type.raw_type == type_t::SELECTION) {
        // This can never be grouped, because we don't construct a selection from an
        // existing datum_stream_t (that might possibly be grouped) unless it's already
        // a selection.  And because ReQL doesn't work that way.  Nonetheless, we just
        // assert that, so that this function being equivalent to
        // as_seq(env)->is_grouped() only uses local reasoning about val_t::as_seq
        // behavior.
        bool ret = selection()->seq->is_grouped();
        rassert(ret == false);
        return ret;
    } else {
        return false;
    }
}

scoped<datum_stream_t> val_t::as_seq(env_t *env) && {
    // See val_t::throw_if_as_seq_type_errors.
    if (type.raw_type == type_t::SEQUENCE) {
        return std::move(sequence());
    } else if (type.raw_type == type_t::SELECTION) {
        return std::move(selection()->seq);
    } else if (type.raw_type == type_t::TABLE_SLICE || type.raw_type == type_t::TABLE) {
        scoped<table_slice_t> slice = std::move(*this).as_table_slice(env);
        return std::move(*slice).as_seq(env, backtrace());
    } else if (type.raw_type == type_t::DATUM) {
        return datum().as_datum_stream(backtrace());
    }
    rcheck_literal_type(env, type_t::SEQUENCE);
    unreachable();
}

void val_t::throw_if_as_seq_type_errors(env_t *env) const {
    // See val_t::as_seq.
    if (type.raw_type == type_t::SEQUENCE) {
        return;
    } else if (type.raw_type == type_t::SELECTION) {
        return;
    } else if (type.raw_type == type_t::TABLE_SLICE || type.raw_type == type_t::TABLE) {
        return;
    } else if (type.raw_type == type_t::DATUM) {
        datum().throw_if_as_datum_stream_type_errors();
        return;
    }
    rcheck_literal_type(env, type_t::SEQUENCE);
    unreachable();

}

counted_t<grouped_data_t> val_t::as_grouped_data(env_t *env) && {
    rcheck_literal_type(env, type_t::GROUPED_DATA);
    return std::move(boost::get<counted_t<grouped_data_t> >(u));
}

counted_t<grouped_data_t> val_t::as_promiscuous_grouped_data(env_t *env) && {
    return ((type.raw_type == type_t::SEQUENCE) && sequence()->is_grouped())
        ? std::move(*sequence()->to_array(env)).as_grouped_data(env)
        : std::move(*this).as_grouped_data(env);
}

counted_t<grouped_data_t> val_t::maybe_as_grouped_data() {
    return type.raw_type == type_t::GROUPED_DATA
        ? boost::get<counted_t<grouped_data_t>>(u)
        : counted_t<grouped_data_t>();
}

counted_t<grouped_data_t> val_t::maybe_as_promiscuous_grouped_data(env_t *env) {
    return ((type.raw_type == type_t::SEQUENCE) && sequence()->is_grouped())
        ? std::move(*sequence()->to_array(env)).as_grouped_data(env)
        : maybe_as_grouped_data();
}

name_string_t val_t::fdb_get_underlying_table_name(env_t *env) const {
    if (type.raw_type == type_t::TABLE) {
        // Forces r.table() term error message to take precedence.
        scoped<table_t> tab = provisional_to_table(env, table());
        return tab->name;
    } else if (type.raw_type == type_t::SELECTION) {
        return selection()->table->name;
    } else if(type.raw_type == type_t::SINGLE_SELECTION) {
        return single_selection()->get_tbl(env)->name;
    } else if (type.raw_type == type_t::TABLE_SLICE) {
        return table_slice()->get_tbl()->name;
    } else {
        r_sanity_check(false);
        unreachable();
    }
}

scoped<selection_t> val_t::as_selection(env_t *env) && {
    if (type.raw_type == type_t::SELECTION) {
        return std::move(selection());
    } else if (type.is_convertible(type_t::TABLE_SLICE)) {
        scoped<table_slice_t> slice = std::move(*this).as_table_slice(env);
        counted_t<table_t> tbl = slice->get_tbl();
        return make_scoped<selection_t>(
            tbl,
            std::move(*slice).as_seq(env, backtrace()));
    }
    rcheck_literal_type(env, type_t::SELECTION);
    unreachable();
}

scoped<single_selection_t> val_t::as_single_selection(env_t *env) && {
    // The env doesn't actually get used, since it would only get used if this was a
    // single selection and the rcheck _failed_.
    rcheck_literal_type(env, type_t::SINGLE_SELECTION);
    return std::move(single_selection());
}

scoped<const func_t> val_t::as_func(env_t *env, function_shortcut_t shortcut) && {
    if (get_type().is_convertible(type_t::FUNC)) {
        r_sanity_check(func().has());
        return std::move(func());
    }

    if (shortcut == NO_SHORTCUT) {
        rcheck_literal_type(env, type_t::FUNC);
        unreachable();
    }

    if (!type.is_convertible(type_t::DATUM)) {
        rcheck_literal_type(env, type_t::FUNC);
        unreachable();
    }

    // Just pull out the backtrace() here in case a future change below uses the rvalue
    // context.
    backtrace_id_t our_bt = backtrace();
    try {
        switch (shortcut) {
        case CONSTANT_SHORTCUT:
            return new_constant_func(as_datum(env), backtrace());
        case GET_FIELD_SHORTCUT:
            return new_get_field_func(as_datum(env), backtrace());
        case PLUCK_SHORTCUT:
            return new_pluck_func(as_datum(env), backtrace());
        case PAGE_SHORTCUT:
            return new_page_func(as_datum(env), backtrace());
        case NO_SHORTCUT:
            // fallthru
        default: unreachable();
        }
    } catch (const datum_exc_t &ex) {
        throw exc_t(ex, our_bt);
    }
}

// QQQ: Move decl up to header, or something, or move code out of db_table.cc, however this shakes out.
counted_t<const db_t> provisional_to_db(
        FDBDatabase *fdb,
        reqlfdb_config_cache *cc,
        const signal_t *interruptor,
        const provisional_db_id &prov_db);

counted_t<const db_t> provisional_to_db(
        env_t *env,
        const provisional_db_id &prov_db) {
    return provisional_to_db(
        env->get_rdb_ctx()->fdb,
        env->get_rdb_ctx()->config_caches.get(),
        env->interruptor,
        prov_db);
}

provisional_db_id val_t::as_prov_db(env_t *env) && {
    rcheck_literal_type(env, type_t::DB);
    return std::move(db());
}

datum_t val_t::as_ptype(env_t *env, const std::string s) const {
    try {
        datum_t d = as_datum(env);
        r_sanity_check(d.has());
        d.rcheck_is_ptype(s);
        return d;
    } catch (const datum_exc_t &e) {
        rfail(e.get_type(), "%s", e.what());
    }
}

bool val_t::as_bool(env_t *env) const {
    try {
        datum_t d = as_datum(env);
        r_sanity_check(d.has());
        return d.as_bool();
    } catch (const datum_exc_t &e) {
        rfail(e.get_type(), "%s", e.what());
    }
}
double val_t::as_num(env_t *env) const {
    try {
        datum_t d = as_datum(env);
        r_sanity_check(d.has());
        return d.as_num();
    } catch (const datum_exc_t &e) {
        rfail(e.get_type(), "%s", e.what());
    }
}
int64_t val_t::as_int(env_t *env) const {
    try {
        datum_t d = as_datum(env);
        r_sanity_check(d.has());
        return d.as_int();
    } catch (const datum_exc_t &e) {
        rfail(e.get_type(), "%s", e.what());
    }
}
datum_string_t val_t::as_str(env_t *env) const {
    try {
        datum_t d = as_datum(env);
        r_sanity_check(d.has());
        return d.as_str();
    } catch (const datum_exc_t &e) {
        rfail(e.get_type(), "%s", e.what());
    }
}



void val_t::rcheck_literal_type(env_t *env, type_t::raw_type_t expected_raw_type) const {
    // God, it is insane that this uses env_t in exc_type.
    // TODO: Change the query language so that this doesn't take env_t and use it in exc_tyep.  (We do use it for fdb_print now, which is less insane.)
    rcheck(
        type.raw_type == expected_raw_type,
        exc_type(env, this),
        strprintf("Expected type %s but found %s:\n%s",
                  type_t(expected_raw_type).name(), type.name(), fdb_print(env).c_str()));
}

std::string val_t::fdb_print(env_t *env) const {
    if (get_type().is_convertible(type_t::DATUM)) {
        return as_datum(env).print();
    } else if (get_type().raw_type == type_t::DB) {
        // Forces consumptive side effect that we want so we fail on db not found error
        // first.  (Forcing eval of the preceding r.db term to be computed (and
        // potentially fail) before the error message using this fdb_print call gets
        // returned.)
        counted_t<const db_t> d = provisional_to_db(env, db());

        return strprintf("db(\"%s\")", d->name.c_str());
    } else if (get_type().is_convertible(type_t::TABLE)) {
        return strprintf("table(\"%s\")", fdb_get_underlying_table_name(env).c_str());
    } else if (get_type().is_convertible(type_t::SELECTION)) {
        return strprintf("SELECTION ON table(%s)",
                         fdb_get_underlying_table_name(env).c_str());
    } else {
        // TODO: Do something smarter here?
        return strprintf("VALUE %s", get_type().name());
    }
}

std::string val_t::fdb_trunc_print(env_t *env) const {
    if (get_type().is_convertible(type_t::DATUM)) {
        return as_datum(env).trunc_print();
    } else {
        std::string s = fdb_print(env);
        if (s.size() > datum_t::trunc_len) {
            s.erase(s.begin() + (datum_t::trunc_len - 3), s.end());
            s += "...";
        }
        return s;
    }
}

} // namespace ql
