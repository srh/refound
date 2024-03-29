// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/btree.hpp"

#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <set>
#include <string>
#include <vector>

#include "concurrency/coro_pool.hpp"
#include "concurrency/new_mutex.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/buffer_group_stream.hpp"
#include "containers/archive/string_stream.hpp"
#include "containers/scoped.hpp"
#include "debug.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/geo/exceptions.hpp"
#include "rdb_protocol/geo/indexing.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/geo_traversal.hpp"
#include "rdb_protocol/pseudo_geometry.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/shards.hpp"
#include "rdb_protocol/table_common.hpp"
#include "rdb_protocol/val.hpp"


ql::serialization_result_t datum_serialize_to_string(const ql::datum_t &datum, std::string *out) {
    write_message_t wm;
    ql::serialization_result_t res =
        datum_serialize(&wm, datum, ql::check_datum_serialization_errors_t::YES);
    if (bad(res)) {
        return res;
    }
    *out = wm.send_to_string();
    return res;
}

ql::serialization_result_t datum_serialize_to_write_message(const ql::datum_t &datum, write_message_t *out) {
    return datum_serialize(out, datum, ql::check_datum_serialization_errors_t::YES);
}

ql::datum_t btree_batched_replacer_t::apply_write_hook(
    const datum_string_t &pkey,
    const ql::datum_t &d,
    const ql::datum_t &res_,
    const ql::datum_t &write_timestamp,
    const counted_t<const ql::func_t> &write_hook) {
    ql::datum_t res = res_;
    if (write_hook.has()) {
        ql::datum_t primary_key;
        if (res.get_type() != ql::datum_t::type_t::R_NULL) {
            primary_key = res.get_field(pkey, ql::throw_bool_t::NOTHROW);
        } else if (d.get_type() != ql::datum_t::type_t::R_NULL) {
            primary_key = d.get_field(pkey, ql::throw_bool_t::NOTHROW);
        }
        if (!primary_key.has()) {
            primary_key = ql::datum_t::null();
        }
        ql::datum_t modified;
        try {
            cond_t non_interruptor;
            ql::env_t write_hook_env(&non_interruptor,
                                     ql::return_empty_normal_batches_t::NO,
                                     reql_version_t::LATEST);

            ql::datum_object_builder_t builder;
            builder.overwrite("primary_key", std::move(primary_key));
            builder.overwrite("timestamp", write_timestamp);

            modified = write_hook->call(&write_hook_env,
                                        std::vector<ql::datum_t>{
                                            std::move(builder).to_datum(),
                                                d,
                                                res})->as_datum(&write_hook_env);
        } catch (ql::exc_t &e) {
            throw ql::exc_t(e.get_type(),
                            strprintf("Error in write hook: %s", e.what()),
                            e.backtrace(),
                            e.dummy_frames());
        } catch (ql::datum_exc_t &e) {
            throw ql::datum_exc_t(e.get_type(),
                                  strprintf("Error in write hook: %s", e.what()));
        }

        rcheck_toplevel(!(res.get_type() == ql::datum_t::type_t::R_NULL &&
                          modified.get_type() != ql::datum_t::type_t::R_NULL),
                        ql::base_exc_t::OP_FAILED,
                        "A write hook function must not turn a deletion into a "
                        "replace/insert.");
        rcheck_toplevel(!(res.get_type() != ql::datum_t::type_t::R_NULL &&
                          modified.get_type() == ql::datum_t::type_t::R_NULL),
                        ql::base_exc_t::OP_FAILED,
                        "A write hook function must not turn a replace/insert "
                        "into a deletion.");
        res = modified;
    }
    return res;
}

std::vector<std::string> expand_geo_key(
        const ql::datum_t &key,
        const store_key_t &primary_key,
        optional<uint64_t> tag_num) {
    // Ignore non-geometry objects in geo indexes.
    // TODO (daniel): This needs to be changed once compound geo index
    // support gets added.
    if (!key.is_ptype(ql::pseudo::geometry_string)) {
        return std::vector<std::string>();
    }

    try {
        std::vector<std::string> grid_keys =
            compute_index_grid_keys(key, GEO_INDEX_GOAL_GRID_CELLS);

        std::vector<std::string> result;
        result.reserve(grid_keys.size());
        for (size_t i = 0; i < grid_keys.size(); ++i) {
            // TODO (daniel): Something else that needs change for compound index
            //   support: We must be able to truncate geo keys and handle such
            //   truncated keys.
            rassert(grid_keys[i].length() <= ql::datum_t::trunc_size(
                        key_to_unescaped_str(primary_key).length()));

            result.push_back(
                ql::datum_t::compose_secondary(
                    grid_keys[i], primary_key, tag_num));
        }

        return result;
    } catch (const geo_exception_t &e) {
        // As things are now, this exception is actually ignored in
        // `compute_keys()`. That's ok, though it would be nice if we could
        // pass on some kind of warning to the user.
        logWRN("Failed to compute grid keys for an index: %s", e.what());
        rfail_target(&key, ql::base_exc_t::LOGIC,
                "Failed to compute grid keys: %s", e.what());
    }
}

void compute_keys(const store_key_t &primary_key,
                  ql::datum_t doc,
                  const sindex_disk_info_t &index_info,
                  std::vector<store_key_t> *keys_out,
                  std::vector<index_pair_t> *cfeed_keys_out) {

    guarantee(keys_out->empty());

    const reql_version_t reql_version =
        index_info.mapping_version_info.latest_compatible_reql_version;

    // Secondary index functions are deterministic (so no need for an rdb_context_t)
    // and evaluated in a pristine environment (without global optargs).
    cond_t non_interruptor;
    ql::env_t sindex_env(&non_interruptor,
                         ql::return_empty_normal_batches_t::NO,
                         reql_version);

    ql::datum_t index =
        index_info.mapping.det_func.compile()->call(&sindex_env, doc)->as_datum(&sindex_env);

    if (index_info.multi == sindex_multi_bool_t::MULTI
        && index.get_type() == ql::datum_t::R_ARRAY) {
        for (uint64_t i = 0; i < index.arr_size(); ++i) {
            const ql::datum_t &skey = index.get(i, ql::THROW);
            if (index_info.geo == sindex_geo_bool_t::GEO) {
                std::vector<std::string> geo_keys = expand_geo_key(skey,
                                                                   primary_key,
                                                                   make_optional(i));
                for (auto it = geo_keys.begin(); it != geo_keys.end(); ++it) {
                    keys_out->emplace_back(*it);
                }
                if (cfeed_keys_out != nullptr) {
                    // For geospatial indexes, we generate multiple keys for the same
                    // index entry. We only pass the smallest one on in order to not get
                    // redundant results on the changefeed.
                    auto min_it = std::min_element(geo_keys.begin(), geo_keys.end());
                    if (min_it != geo_keys.end()) {
                        cfeed_keys_out->push_back(
                            std::make_pair(skey, std::move(*min_it)));
                    }
                }
            } else {
                try {
                    std::string store_key =
                        skey.print_secondary(primary_key,
                                             make_optional(i));
                    keys_out->emplace_back(store_key);
                    if (cfeed_keys_out != nullptr) {
                        cfeed_keys_out->push_back(
                            std::make_pair(skey, std::move(store_key)));
                    }
                } catch (const ql::base_exc_t &e) {
                    // One of the values couldn't be converted to an index key.
                    // Ignore it and move on to the next one.
                }
            }
        }
    } else {
        if (index_info.geo == sindex_geo_bool_t::GEO) {
            std::vector<std::string> geo_keys = expand_geo_key(index,
                                                               primary_key,
                                                               r_nullopt);
            for (auto it = geo_keys.begin(); it != geo_keys.end(); ++it) {
                keys_out->emplace_back(*it);
            }
            if (cfeed_keys_out != nullptr) {
                // For geospatial indexes, we generate multiple keys for the same
                // index entry. We only pass the smallest one on in order to not get
                // redundant results on the changefeed.
                auto min_it = std::min_element(geo_keys.begin(), geo_keys.end());
                if (min_it != geo_keys.end()) {
                    cfeed_keys_out->push_back(
                        std::make_pair(index, std::move(*min_it)));
                }
            }
        } else {
            std::string store_key =
                index.print_secondary(primary_key, r_nullopt);
            keys_out->emplace_back(store_key);
            if (cfeed_keys_out != nullptr) {
                cfeed_keys_out->push_back(
                    std::make_pair(index, std::move(store_key)));
            }
        }
    }
}


