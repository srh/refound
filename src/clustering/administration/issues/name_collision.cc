// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/issues/name_collision.hpp"

#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/servers/config_client.hpp"
#include "clustering/administration/tables/database_metadata.hpp"
#include "clustering/administration/tables/table_metadata.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
#include "rdb_protocol/configured_limits.hpp"

// Base issue for all name collisions (server, database, and table)
class name_collision_issue_t : public issue_t {
public:
    name_collision_issue_t(const issue_id_t &_issue_id,
                           const name_string_t &_name,
                           const std::vector<std::string> &_collided_ids);
    bool is_critical() const { return true; }

protected:
    const name_string_t name;
    const std::vector<std::string> collided_ids;
};

// Issue for server name collisions
class server_name_collision_issue_t : public name_collision_issue_t {
public:
    explicit server_name_collision_issue_t(
        const name_string_t &_name,
        const std::vector<server_id_t> &_collided_ids);
    const datum_string_t &get_name() const { return server_name_collision_issue_type; }

private:
    static const datum_string_t server_name_collision_issue_type;
    static const issue_id_t base_issue_id;
    bool build_info_and_description(
        const metadata_t &metadata,
        server_config_client_t *server_config_client,
        table_meta_client_t *table_meta_client,
        admin_identifier_format_t identifier_format,
        ql::datum_t *info_out,
        datum_string_t *description_out) const;
};

const datum_string_t server_name_collision_issue_t::server_name_collision_issue_type =
    datum_string_t("server_name_collision");
const uuid_u server_name_collision_issue_t::base_issue_id =
    str_to_uuid("a8bf71c6-a178-43ea-b25d-73a13619c8f0");

name_collision_issue_t::name_collision_issue_t(
        const issue_id_t &_issue_id,
        const name_string_t &_name,
        const std::vector<std::string> &_collided_ids) :
    issue_t(_issue_id),
    name(_name),
    collided_ids(_collided_ids) { }

void generic_build_info_and_description(
        const std::string &long_type_plural,
        const char *short_type_singular,
        const char *system_table_name,
        const name_string_t &name,
        const std::vector<std::string> &ids,
        ql::datum_t *info_out,
        datum_string_t *description_out) {
    ql::datum_object_builder_t builder;
    ql::datum_array_builder_t ids_builder(ql::configured_limits_t::unlimited);
    std::string ids_str;
    for (auto const &id : ids) {
        ids_builder.add(ql::datum_t(datum_string_t(id)));
        if (!ids_str.empty()) {
            ids_str += ", ";
        }
        ids_str += id;
    }
    builder.overwrite("name", convert_name_to_datum(name));
    builder.overwrite("ids", std::move(ids_builder).to_datum());
    *info_out = std::move(builder).to_datum();
    *description_out = datum_string_t(strprintf(
        "There are multiple %s named `%s`. RethinkDB requires that every %s have a "
        "unique name. Please update the `name` field of the conflicting %ss' documents "
        "in the `rethinkdb.%s` system table.\n\nThe UUIDs of the conflicting %ss are as "
        "follows: %s",
        long_type_plural.c_str(), name.c_str(), short_type_singular, short_type_singular,
        system_table_name, short_type_singular, ids_str.c_str()));
}

// Helper functions for converting IDs to strings
std::vector<std::string> server_ids_to_str_ids(const std::vector<server_id_t> &ids) {
    std::vector<std::string> res;
    res.reserve(ids.size());
    for (const auto &id : ids) {
        res.push_back(id.print());
    }
    return res;
}
std::vector<std::string> table_ids_to_str_ids(const std::vector<namespace_id_t> &ids) {
    std::vector<std::string> res;
    res.reserve(ids.size());
    for (const auto &id : ids) {
        res.push_back(uuid_to_str(id));
    }
    return res;
}
std::vector<std::string> database_ids_to_str_ids(const std::vector<database_id_t> &ids) {
    std::vector<std::string> res;
    res.reserve(ids.size());
    for (const auto &id : ids) {
        res.push_back(uuid_to_str(id));
    }
    return res;
}

server_name_collision_issue_t::server_name_collision_issue_t(
        const name_string_t &_name,
        const std::vector<server_id_t> &_collided_ids) :
    name_collision_issue_t(from_hash(base_issue_id, _name),
                           _name,
                           server_ids_to_str_ids(_collided_ids)) { }

bool server_name_collision_issue_t::build_info_and_description(
        UNUSED const metadata_t &metadata,
        UNUSED server_config_client_t *server_config_client,
        UNUSED table_meta_client_t *table_meta_client,
        UNUSED admin_identifier_format_t identifier_format,
        ql::datum_t *info_out,
        datum_string_t *description_out) const {
    generic_build_info_and_description(
        "servers", "server", "server_config", name, collided_ids,
        info_out, description_out);
    return true;
}

name_collision_issue_tracker_t::name_collision_issue_tracker_t(
            server_config_client_t *_server_config_client) :
    server_config_client(_server_config_client) { }

name_collision_issue_tracker_t::~name_collision_issue_tracker_t() { }

void find_server_duplicates(
        watchable_map_t<server_id_t, server_config_versioned_t> *servers,
        std::vector<scoped_ptr_t<issue_t> > *issues_out) {
    std::map<name_string_t, std::vector<server_id_t> > name_counts;
    servers->read_all(
    [&](const server_id_t &sid, const server_config_versioned_t *config) {
        name_counts[config->config.name].push_back(sid);
    });
    for (auto const &it : name_counts) {
        if (it.second.size() > 1) {
            issues_out->push_back(scoped_ptr_t<issue_t>(
                new server_name_collision_issue_t(it.first, it.second)));
        }
    }
}

std::vector<scoped_ptr_t<issue_t> > name_collision_issue_tracker_t::get_issues(
        UNUSED const signal_t *interruptor) const {
    std::vector<scoped_ptr_t<issue_t> > issues;

    find_server_duplicates(server_config_client->get_server_config_map(), &issues);

    return issues;
}
