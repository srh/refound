// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/tables/table_metadata.hpp"

#include "clustering/administration/tables/database_metadata.hpp"
#include "clustering/id_types.hpp"
#include "containers/archive/archive.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/stl_types.hpp"
#include "containers/archive/versioned.hpp"
#include "rdb_protocol/protocol.hpp"

// We start with an empty object, not null -- because a good user would set fields of
// that object.
user_data_t default_user_data() {
    return user_data_t{ql::datum_t::empty_object()};
}

RDB_MAKE_SERIALIZABLE_1(user_data_t, datum);

RDB_IMPL_EQUALITY_COMPARABLE_1(user_data_t, datum);

RDB_DECLARE_SERIALIZABLE(table_config_t);


class table_config_and_shards_change_t::apply_change_visitor_t
    : public boost::static_visitor<bool> {
public:
    explicit apply_change_visitor_t(
            table_config_and_shards_t *_table_config_and_shards)
        : table_config_and_shards(_table_config_and_shards) { }

    result_type operator()(
            const set_table_config_and_shards_t &set_table_config_and_shards) const {
        *table_config_and_shards =
            set_table_config_and_shards.new_config_and_shards;
        return true;
    }

    result_type operator()(const write_hook_create_t &write_hook_create) const {
        table_config_and_shards->config.write_hook.set(write_hook_create.config);
        return true;
    }

    result_type operator()(UNUSED const write_hook_drop_t &write_hook_drop) const {
        table_config_and_shards->config.write_hook = r_nullopt;
        return true;
    }

    result_type operator()(const sindex_create_t &sindex_create) const {
        auto pair = table_config_and_shards->config.sindexes.insert(
            std::make_pair(sindex_create.name, sindex_create.config));
        return pair.second;
    }

    result_type operator()(const sindex_drop_t &sindex_drop) const {
        auto size = table_config_and_shards->config.sindexes.erase(sindex_drop.name);
        return size == 1;
    }

    result_type operator()(const sindex_rename_t &sindex_rename) const {
        if (table_config_and_shards->config.sindexes.count(
                sindex_rename.name) == 0) {
            /* The index `sindex_rename.name` does not exist. */
            return false;
        }
        if (sindex_rename.name != sindex_rename.new_name) {
            if (table_config_and_shards->config.sindexes.count(
                        sindex_rename.new_name) == 1 &&
                    sindex_rename.overwrite == false) {
                /* The index `sindex_rename.new_name` already exits and should not be
                overwritten. */
                return false;
            } else {
                table_config_and_shards->config.sindexes[sindex_rename.new_name] =
                    table_config_and_shards->config.sindexes.at(sindex_rename.name);
                table_config_and_shards->config.sindexes.erase(sindex_rename.name);
            }
        }
        return true;
    }

private:
    table_config_and_shards_t *table_config_and_shards;
};

/* Note, it's important that `apply_change` does not change
`table_config_and_shards` if it returns false. */
bool table_config_and_shards_change_t::apply_change(table_config_and_shards_t *table_config_and_shards) const {
    return boost::apply_visitor(
        apply_change_visitor_t(table_config_and_shards), change);
}

bool  table_config_and_shards_change_t::name_and_database_equal(const table_basic_config_t &table_basic_config) const {
    const set_table_config_and_shards_t *set_table_config_and_shards =
        boost::get<set_table_config_and_shards_t>(&change);
    if (set_table_config_and_shards == nullptr) {
        return true;
    } else {
        return set_table_config_and_shards->new_config_and_shards.config.basic.name == table_basic_config.name &&
            set_table_config_and_shards->new_config_and_shards.config.basic.database == table_basic_config.database;
    }
}


RDB_IMPL_SERIALIZABLE_3_SINCE_v2_1(table_basic_config_t,
    name, database, primary_key);
RDB_IMPL_EQUALITY_COMPARABLE_3(table_basic_config_t,
    name, database, primary_key);

RDB_IMPL_SERIALIZABLE_4_SINCE_v2_5(table_config_t,
    basic, sindexes, write_hook, user_data);

// TODO: Do we still need equality comparisons?  At some point.
RDB_IMPL_EQUALITY_COMPARABLE_4(table_config_t,
    basic, write_hook, sindexes, user_data);

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(table_config_and_shards_t, config);
RDB_IMPL_EQUALITY_COMPARABLE_1(table_config_and_shards_t, config);

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(
    table_config_and_shards_change_t::set_table_config_and_shards_t,
    new_config_and_shards);
RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(table_config_and_shards_change_t::sindex_create_t,
    name, config);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(table_config_and_shards_change_t::sindex_drop_t,
    name);
RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(table_config_and_shards_change_t::sindex_rename_t,
    name, new_name, overwrite);

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(table_config_and_shards_change_t::write_hook_create_t, config);
RDB_IMPL_SERIALIZABLE_0_FOR_CLUSTER(table_config_and_shards_change_t::write_hook_drop_t);

RDB_IMPL_SERIALIZABLE_1_SINCE_v1_13(database_semilattice_metadata_t, name);
RDB_IMPL_SEMILATTICE_JOINABLE_1(database_semilattice_metadata_t, name);
RDB_IMPL_EQUALITY_COMPARABLE_1(database_semilattice_metadata_t, name);

RDB_IMPL_SERIALIZABLE_1_SINCE_v1_13(databases_semilattice_metadata_t, databases);
RDB_IMPL_SEMILATTICE_JOINABLE_1(databases_semilattice_metadata_t, databases);
RDB_IMPL_EQUALITY_COMPARABLE_1(databases_semilattice_metadata_t, databases);

