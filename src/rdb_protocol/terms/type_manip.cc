// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/terms.hpp"

#include <algorithm>
#include <map>
#include <string>

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/tables/table_metadata.hpp"
#include "clustering/id_types.hpp"
#include "fdb/retry_loop.hpp"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rdb_protocol/base_table.hpp"
#include "rdb_protocol/datum_stream.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/math_utils.hpp"
#include "rdb_protocol/op.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

namespace ql {

// TODO: Make this whole file not suck.

// Some Problems:
// * The method of constructing a canonical type from supertype * MAX_TYPE +
//   subtype is brittle.
// * Everything is done with nested ifs.  Ideally we'd build some sort of graph
//   structure and walk it.

static const int MAX_TYPE = 10;

static const int DB_TYPE = val_t::type_t::DB * MAX_TYPE;
static const int TABLE_TYPE = val_t::type_t::TABLE * MAX_TYPE;
static const int TABLE_SLICE_TYPE = val_t::type_t::TABLE_SLICE * MAX_TYPE;
static const int SELECTION_TYPE = val_t::type_t::SELECTION * MAX_TYPE;
static const int ARRAY_SELECTION_TYPE = SELECTION_TYPE + datum_t::R_ARRAY;
static const int SEQUENCE_TYPE = val_t::type_t::SEQUENCE * MAX_TYPE;
static const int SINGLE_SELECTION_TYPE = val_t::type_t::SINGLE_SELECTION * MAX_TYPE;
static const int DATUM_TYPE = val_t::type_t::DATUM * MAX_TYPE;
static const int FUNC_TYPE = val_t::type_t::FUNC * MAX_TYPE;
static const int GROUPED_DATA_TYPE = val_t::type_t::GROUPED_DATA * MAX_TYPE;

static const int R_NULL_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::R_NULL;
static const int R_BINARY_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::R_BINARY;
static const int R_BOOL_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::R_BOOL;
static const int R_NUM_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::R_NUM;
static const int R_STR_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::R_STR;
static const int R_ARRAY_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::R_ARRAY;
static const int R_OBJECT_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::R_OBJECT;
static const int MINVAL_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::MINVAL;
static const int MAXVAL_TYPE = val_t::type_t::DATUM * MAX_TYPE + datum_t::MAXVAL;

class coerce_map_t {
public:
    coerce_map_t() {
        map["DB"] = DB_TYPE;
        map["TABLE"] = TABLE_TYPE;
        map["TABLE_SLICE"] = TABLE_SLICE_TYPE;
        map["SELECTION<STREAM>"] = SELECTION_TYPE;
        map["SELECTION<ARRAY>"] = ARRAY_SELECTION_TYPE;
        map["STREAM"] = SEQUENCE_TYPE;
        map["SELECTION<OBJECT>"] = SINGLE_SELECTION_TYPE;
        map["DATUM"] = DATUM_TYPE;
        map["FUNCTION"] = FUNC_TYPE;
        map["GROUPED_DATA"] = GROUPED_DATA_TYPE;
        CT_ASSERT(val_t::type_t::GROUPED_DATA < MAX_TYPE);

        map["NULL"] = R_NULL_TYPE;
        map["BINARY"] = R_BINARY_TYPE;
        map["BOOL"] = R_BOOL_TYPE;
        map["NUMBER"] = R_NUM_TYPE;
        map["STRING"] = R_STR_TYPE;
        map["ARRAY"] = R_ARRAY_TYPE;
        map["OBJECT"] = R_OBJECT_TYPE;
        map["MINVAL"] = MINVAL_TYPE;
        map["MAXVAL"] = MAXVAL_TYPE;
        CT_ASSERT(datum_t::MAXVAL < MAX_TYPE);

        for (std::map<std::string, int>::iterator
                 it = map.begin(); it != map.end(); ++it) {
            rmap[it->second] = it->first;
        }
        guarantee(map.size() == rmap.size());
    }
    int get_type(const std::string &s, const rcheckable_t *caller) const {
        std::map<std::string, int>::const_iterator it = map.find(s);
        rcheck_target(caller,
                      it != map.end(),
                      base_exc_t::LOGIC,
                      strprintf("Unknown Type: %s", s.c_str()));
        return it->second;
    }
    std::string get_name(int type) const {
        std::map<int, std::string>::const_iterator it = rmap.find(type);
        r_sanity_check(it != rmap.end());
        return it->second;
    }
private:
    // TODO: Unordered map?
    std::map<std::string, int> map;
    std::map<int, std::string> rmap;

    // These functions are here so that if you add a new type you have to update
    // this file.
    // THINGS TO DO:
    // * Update the coerce_map_t.
    // * Add the various coercions.
    // * Update the `switch` in `info_term_t`.
    // * !!! CHECK WHETHER WE HAVE MORE THAN MAX_TYPE TYPES AND INCREASE !!!
    //   !!! MAX_TYPE IF WE DO                                           !!!
    void NOCALL_ct_catch_new_types(val_t::type_t::raw_type_t t, datum_t::type_t t2) {
        switch (t) {
        case val_t::type_t::DB:
        case val_t::type_t::TABLE:
        case val_t::type_t::TABLE_SLICE:
        case val_t::type_t::SELECTION:
        case val_t::type_t::SEQUENCE:
        case val_t::type_t::SINGLE_SELECTION:
        case val_t::type_t::DATUM:
        case val_t::type_t::FUNC:
        case val_t::type_t::GROUPED_DATA:
        default: break;
        }
        switch (t2) {
        case datum_t::UNINITIALIZED:
        case datum_t::R_NULL:
        case datum_t::R_BINARY:
        case datum_t::R_BOOL:
        case datum_t::R_NUM:
        case datum_t::R_STR:
        case datum_t::R_ARRAY:
        case datum_t::R_OBJECT:
        case datum_t::MINVAL:
        case datum_t::MAXVAL:
        default: break;
        }
    }
};

static const coerce_map_t _coerce_map;
static int get_type(std::string s, const rcheckable_t *caller) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    return _coerce_map.get_type(s, caller);
}
static std::string get_name(int type) {
    return _coerce_map.get_name(type);
}

static val_t::type_t::raw_type_t supertype(int type) {
    return static_cast<val_t::type_t::raw_type_t>(type / MAX_TYPE);
}
static datum_t::type_t subtype(int type) {
    return static_cast<datum_t::type_t>(type % MAX_TYPE);
}
static int merge_types(int supertype, int subtype) {
    return supertype * MAX_TYPE + subtype;
}

class coerce_term_t : public op_term_t {
public:
    coerce_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        scoped_ptr_t<val_t> val = args->arg(env, 0);
        val_t::type_t opaque_start_type = val->get_type();
        int start_supertype = opaque_start_type.raw_type;
        int start_subtype = 0;
        if (opaque_start_type.is_convertible(val_t::type_t::DATUM)) {
            start_supertype = val_t::type_t::DATUM;
            start_subtype = val->as_datum(env).get_type();
        }
        int start_type = merge_types(start_supertype, start_subtype);

        std::string end_type_name = args->arg(env, 1)->as_str(env).to_std();
        int end_type = get_type(end_type_name, this);

        // Identity
        if ((!subtype(end_type)
             && opaque_start_type.is_convertible(supertype(end_type)))
            || start_type == end_type) {
            return val;
        }

        // * -> BOOL
        if (end_type == R_BOOL_TYPE) {
            if (start_type == R_NULL_TYPE) {
                return new_val(datum_t::boolean(false));
            }
            return new_val(datum_t::boolean(true));
        }

        // DATUM -> *
        if (opaque_start_type.is_convertible(val_t::type_t::DATUM)) {
            datum_t d = val->as_datum(env);
            // DATUM -> DATUM
            if (supertype(end_type) == val_t::type_t::DATUM) {
                if (start_type == R_BINARY_TYPE && end_type == R_STR_TYPE) {
                    return new_val(datum_t::utf8(d.as_binary()));
                }
                if (start_type == R_STR_TYPE && end_type == R_BINARY_TYPE) {
                    return new_val(datum_t::binary(d.as_str()));
                }

                // DATUM -> STR
                if (end_type == R_STR_TYPE) {
                    if (true) {
                        rapidjson::StringBuffer buffer;
                        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                        d.write_json(&writer);
                        return new_val(datum_t(
                            datum_string_t(buffer.GetSize(), buffer.GetString())));
                    }
                }

                // OBJECT -> ARRAY
                if (start_type == R_OBJECT_TYPE && end_type == R_ARRAY_TYPE) {
                    std::vector<datum_t> arr;
                    arr.reserve(d.obj_size());
                    for (size_t i = 0; i < d.obj_size(); ++i) {
                        std::pair<datum_string_t, datum_t> pair_in = d.get_pair(i);
                        std::vector<datum_t> pair_out;
                        pair_out.reserve(2);
                        pair_out.push_back(datum_t(pair_in.first));
                        pair_out.push_back(pair_in.second);
                        arr.push_back(datum_t(std::move(pair_out), env->env->limits()));
                    }
                    return new_val(datum_t(std::move(arr), env->env->limits()));
                }

                // STR -> NUM
                if (start_type == R_STR_TYPE && end_type == R_NUM_TYPE) {
                    const datum_string_t &s = d.as_str();
                    double dbl;
                    char end; // Used to ensure that there's no trailing garbage.
                    if (sscanf(s.to_std().c_str(), "%lf%c", &dbl, &end) == 1) {
                        return new_val(datum_t(dbl));
                    } else {
                        rfail(base_exc_t::LOGIC, "Could not coerce `%s` to NUMBER.",
                              s.to_std().c_str());
                    }
                }
            }
            // TODO: Object to sequence?
        }

        if (opaque_start_type.is_convertible(val_t::type_t::SEQUENCE)
            && !(start_supertype == val_t::type_t::DATUM
                 && start_type != R_ARRAY_TYPE)) {
            scoped<datum_stream_t> ds;
            try {
                ds = std::move(*val).as_seq(env->env);
            } catch (const base_exc_t &e) {
                rfail(base_exc_t::LOGIC,
                      "Cannot coerce %s to %s (failed to produce intermediate stream).",
                      get_name(start_type).c_str(), get_name(end_type).c_str());
            }

            // SEQUENCE -> ARRAY
            if (end_type == R_ARRAY_TYPE || end_type == DATUM_TYPE) {
                return ds->to_array(env->env);
            }

            // SEQUENCE -> OBJECT
            if (end_type == R_OBJECT_TYPE) {
                datum_object_builder_t obj;
                batchspec_t batchspec
                    = batchspec_t::user(batch_type_t::TERMINAL, env->env);
                {
                    profile::sampler_t sampler("Coercing to object.", env->env->trace);
                    datum_t pair;
                    while (pair = ds->next(env->env, batchspec), pair.has()) {
                        rcheck(pair.arr_size() == 2,
                               base_exc_t::LOGIC,
                               strprintf("Expected array of size 2, but got size %zu.",
                                         pair.arr_size()));
                        datum_string_t key = pair.get(0).as_str();
                        datum_t keyval = pair.get(1);
                        bool b = obj.add(key, keyval);
                        rcheck(!b, base_exc_t::LOGIC,
                               strprintf("Duplicate key %s in coerced object.  "
                                         "(got %s and %s as values)",
                                         datum_t(key).print().c_str(),
                                         obj.at(key).trunc_print().c_str(),
                                         keyval.trunc_print().c_str()));
                        sampler.new_sample();
                    }
                }
                return new_val(std::move(obj).to_datum());
            }
        }

        // This used to be rfail_typed_target, but because that macro can't generally
        // take an env, we manually call exc_type, pass it to rfail_target.
        rfail_target(val, exc_type(env->env, val.get()),
            "Cannot coerce %s to %s.",
             get_name(start_type).c_str(), get_name(end_type).c_str());
    }
    virtual const char *name() const { return "coerce_to"; }
};

class ungroup_term_t : public op_term_t {
public:
    ungroup_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<grouped_data_t> groups
            = args->arg(env, 0)->as_promiscuous_grouped_data(env->env);
        std::vector<datum_t> v;
        v.reserve(groups->size());

        for (auto &&pair : *groups) {
            r_sanity_check(pair.first.has() && pair.second.has());
            std::map<datum_string_t, datum_t> m =
                {{datum_string_t("group"), pair.first},
                 {datum_string_t("reduction"), std::move(pair.second)}};
            v.push_back(datum_t(std::move(m)));
        }
        return new_val(datum_t(std::move(v), env->env->limits()));
    }
    virtual const char *name() const { return "ungroup"; }
    virtual bool can_be_grouped() const { return false; }
};

int val_type(env_t *env, const scoped_ptr_t<val_t> &v) {
    int t = v->get_type().raw_type * MAX_TYPE;
    if (t == DATUM_TYPE) {
        t += v->as_datum(env).get_type();
    } else if (t == SELECTION_TYPE) {
        if (v->selection()->seq->is_array()) {
            t += datum_t::R_ARRAY;
        }
    }
    return t;
}

datum_t typename_of(env_t *env, const scoped_ptr_t<val_t> &v) {
    if (v->get_type().get_raw_type() == val_t::type_t::DATUM) {
        datum_t d = v->as_datum(env);
        return datum_t(datum_string_t(d.get_type_name()));
    } else if (v->is_grouped_seq()) {
        return datum_t(datum_string_t("GROUPED_STREAM"));
    } else {
        return datum_t(datum_string_t(get_name(val_type(env, v))));
    }
}

class typeof_term_t : public op_term_t {
public:
    typeof_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        return new_val(typename_of(env->env, args->arg(env, 0)));
    }
    virtual const char *name() const { return "typeof"; }
    virtual bool can_be_grouped() const { return false; }
};

class info_term_t : public op_term_t {
public:
    info_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        return new_val(val_info(env->env, args->arg(env, 0)));
    }

    static MUST_USE bool add_db_info(datum_object_builder_t *onto,
        const name_string_t &db_name, const database_id_t &db_id) {
        bool b = onto->add("name", datum_t(db_name.str()));
        b |= onto->add("id", datum_t(uuid_to_str(db_id)));
        return b;
    }

    // TODO: Remove this eventually.
    static MUST_USE bool add_table_info(
            FDBDatabase *fdb,
            const signal_t *interruptor,
            datum_object_builder_t *onto,
            const table_t *table) {
        r_sanity_check(!table->get_id().value.is_nil());

        // NNN: Handle system table case.
        table_config_t config;
        fdb_error_t loop_err = txn_retry_loop_coro(fdb,
                interruptor, [&](FDBTransaction *txn) {
            config = config_cache_get_table_config(
                txn, table->tbl->cv.assert_nonempty() /* OOO: what if system table? */, table->get_id(), interruptor);
        });
        guarantee_fdb_TODO(loop_err, "info term, table, retry loop failed");

        bool b = onto->add("name", datum_t(table->name.str()));
        b |= onto->add("primary_key", datum_t(table->get_pkey()));
        {
            datum_object_builder_t db_info;
            // QQQ: Test that this is "DB" or refactor a tad...
            bool ign = db_info.add("type", datum_t(get_name(DB_TYPE)));
            ign = add_db_info(&db_info, table->db->name, table->db->id);
            (void)ign;
            b |= onto->add("db", std::move(db_info).to_datum());
        }
        b |= onto->add("id", datum_t(uuid_to_str(table->get_id())));
        /* TODO: Add doc_count_estimates, which is an array of doubles, of size
           num_shards. */
        // b |= onto->add("doc_count_estimates", std::move(arr).to_datum());

        {
            ql::datum_array_builder_t res(ql::configured_limits_t::unlimited);
            for (const auto &pair : config.sindexes) {
                res.add(ql::datum_t(datum_string_t(pair.first)));
            }
            res.sort();
            b |= onto->add("indexes", std::move(res).to_datum());
        }
        return b;
    }

    static MUST_USE bool add_table_info(
            FDBDatabase *fdb,
            const signal_t *interruptor,
            datum_object_builder_t *onto,
            const provisional_table_id &prov_table) {
        namespace_id_t table_id;
        table_config_t config;
        if (prov_table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            optional<namespace_id_t> maybe_table_id = artificial_reql_cluster_interface_t::get_table_id(prov_table.table_name);
            if (!maybe_table_id.has_value()) {
                rfail_prov_table_dne(prov_table);
            }
            table_id = *maybe_table_id;
            // Just set the information that gets used below.  config.sindexes remains
            // empty, because artificial tables have no sindexes.
            config.basic.database = artificial_reql_cluster_interface_t::database_id;
            config.basic.primary_key = artificial_table_fdb_backend_t::get_primary_key_name();
        } else {
            std::pair<namespace_id_t, table_config_t> info;
            fdb_error_t loop_err = txn_retry_loop_coro(fdb,
                    interruptor, [&](FDBTransaction *txn) {
                info = expect_retrieve_table(txn, prov_table, interruptor).ci_value;
            });
            guarantee_fdb_TODO(loop_err, "info term, table, retry loop failed");
            table_id = info.first;
            config = std::move(info.second);
        }

        bool b = onto->add("name", datum_t(prov_table.table_name.str()));
        b |= onto->add("primary_key", datum_t(config.basic.primary_key));
        {
            datum_object_builder_t db_info;
            // QQQ: Test that this is "DB" or refactor a tad...
            bool ign = db_info.add("type", datum_t(get_name(DB_TYPE)));
            ign = add_db_info(&db_info, prov_table.prov_db.db_name, config.basic.database);
            (void)ign;
            b |= onto->add("db", std::move(db_info).to_datum());
        }
        b |= onto->add("id", datum_t(uuid_to_str(table_id)));
        /* TODO: Add doc_count_estimates, which is an array of doubles, of size
           num_shards. */
        // b |= onto->add("doc_count_estimates", std::move(arr).to_datum());

        {
            ql::datum_array_builder_t res(ql::configured_limits_t::unlimited);
            for (const auto &pair : config.sindexes) {
                res.add(ql::datum_t(datum_string_t(pair.first)));
            }
            res.sort();
            b |= onto->add("indexes", std::move(res).to_datum());
        }
        return b;
    }

    static ql::datum_t table_info_datum(
            FDBDatabase *fdb,
            const signal_t *interruptor,
            const table_t *table) {
        datum_object_builder_t table_info;
        bool b = table_info.add("type", datum_t(get_name(TABLE_TYPE)));
        b |= add_table_info(fdb, interruptor, &table_info, table);
        r_sanity_check(!b);
        return std::move(table_info).to_datum();
    }

    datum_t val_info(env_t *env, scoped_ptr_t<val_t> v) const {
        datum_object_builder_t info;
        int type = val_type(env, v);
        bool b = info.add("type", typename_of(env, v));

        switch (type) {
        case DB_TYPE: {
            provisional_db_id database = v->as_prov_db(env);

            database_id_t db_id;
            if (database.db_name == artificial_reql_cluster_interface_t::database_name) {
                db_id = artificial_reql_cluster_interface_t::database_id;
            } else {
                fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb,
                        env->interruptor, [&](FDBTransaction *txn) {
                    // TODO: Read-only txn
                    db_id = expect_retrieve_db(txn, database, env->interruptor);
                });
                guarantee_fdb_TODO(loop_err, "info term, db, retry loop failed");
            }

            b |= add_db_info(&info, database.db_name, db_id);
        } break;
        case TABLE_TYPE: {
            provisional_table_id table = v->as_prov_table(env);
            b |= add_table_info(env->get_rdb_ctx()->fdb, env->interruptor,
                &info, table);
        } break;
        case TABLE_SLICE_TYPE: {
            scoped<table_slice_t> ts = std::move(*v).as_table_slice(env);
            b |= info.add("table", table_info_datum(
                env->get_rdb_ctx()->fdb, env->interruptor, ts->get_tbl().get()));
            b |= info.add("index",
                          ts->idx.has_value() ? datum_t(ts->idx->c_str()) : datum_t::null());
            switch (ts->sorting) {
            case sorting_t::UNORDERED:
                b |= info.add("sorting", datum_t("UNORDERED"));
                break;
            case sorting_t::ASCENDING:
                b |= info.add("sorting", datum_t("ASCENDING"));
                break;
            case sorting_t::DESCENDING:
                b |= info.add("sorting", datum_t("DESCENDING"));
                break;
            default: unreachable();
            }
            if (ts->bounds.left_bound.get_type() == datum_t::type_t::MINVAL) {
                b |= info.add("left_bound_type", datum_t("unbounded"));
            } else if (ts->bounds.left_bound.get_type() == datum_t::type_t::MAXVAL) {
                b |= info.add("left_bound_type", datum_t("unachievable"));
            } else {
                b |= info.add("left_bound", ts->bounds.left_bound);
                b |= info.add("left_bound_type",
                              datum_t(ts->bounds.left_bound_type == key_range_t::open
                                      ? "open" : "closed"));
            }
            if (ts->bounds.right_bound.get_type() == datum_t::type_t::MAXVAL) {
                b |= info.add("right_bound_type", datum_t("unbounded"));
            } else if (ts->bounds.right_bound.get_type() == datum_t::type_t::MINVAL) {
                b |= info.add("right_bound_type", datum_t("unachievable"));
            } else {
                b |= info.add("right_bound", ts->bounds.right_bound);
                b |= info.add("right_bound_type",
                              datum_t(ts->bounds.right_bound_type == key_range_t::open
                                      ? "open" : "closed"));
            }
        } break;
        case SELECTION_TYPE: {
            scoped<selection_t> sel = std::move(*v).as_selection(env);
            b |= info.add("table", table_info_datum(
                env->get_rdb_ctx()->fdb, env->interruptor,
                sel->table.get()));
        } break;
        case ARRAY_SELECTION_TYPE: {
            scoped<selection_t> sel = std::move(*v).as_selection(env);
            b |= info.add("table", table_info_datum(
                env->get_rdb_ctx()->fdb, env->interruptor,
                sel->table.get()));
        } break;
        case SEQUENCE_TYPE: {
            // TODO: Remove this?

            // This case seems already handled in typename_of.  So we'd just overwrite
            // "GROUPED_STREAM" with "GROUPED_STREAM."  Also, as_seq has no side effects
            // because the val type is type_t::SEQUENCE, which just does returns
            // std::move(sequence()) without any chance of throwing an error.
            if (std::move(*v).as_seq(env)->is_grouped()) {
                // So we have this assertion.
                rassert(info.at(datum_string_t("type")) == datum_t("GROUPED_STREAM"));

                bool res = info.add("type", datum_t("GROUPED_STREAM"));
                r_sanity_check(res);
            }
        } break;
        case SINGLE_SELECTION_TYPE: {
            scoped<single_selection_t> single_sel = std::move(*v).as_single_selection(env);
            b |= info.add("table", table_info_datum(
                env->get_rdb_ctx()->fdb, env->interruptor,
                single_sel->get_tbl().get()));
        } break;

        case FUNC_TYPE: {
            b |= info.add("source_code",
                datum_t(datum_string_t(v->as_func(env)->print_source())));
        } break;

        case GROUPED_DATA_TYPE: break; // No more info

        case R_BINARY_TYPE:
            b |= info.add("count",
                          datum_t(safe_to_double(v->as_datum(env).as_binary().size())));
            // fallthru

        case R_BOOL_TYPE:   // fallthru
        case R_NUM_TYPE:    // fallthru
        case R_STR_TYPE:    // fallthru
        case R_ARRAY_TYPE:  // fallthru
        case R_OBJECT_TYPE: // fallthru
        case DATUM_TYPE: {
            b |= info.add("value",
                          datum_t(datum_string_t(
                              v->as_datum(env).print())));
        } // fallthru
        case R_NULL_TYPE:   // fallthru
        case MINVAL_TYPE:   // fallthru
        case MAXVAL_TYPE: break;

        default: r_sanity_check(false);
        }
        r_sanity_check(!b);
        return std::move(info).to_datum();
    }

    virtual const char *name() const { return "info"; }
    virtual bool can_be_grouped() const { return false; }
};

counted_t<term_t> make_coerce_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<coerce_term_t>(env, term);
}
counted_t<term_t> make_ungroup_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<ungroup_term_t>(env, term);
}
counted_t<term_t> make_typeof_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<typeof_term_t>(env, term);
}
counted_t<term_t> make_info_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<info_term_t>(env, term);
}

} // namespace ql
