// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_METADATA_HPP_
#define CLUSTERING_ADMINISTRATION_METADATA_HPP_

#include <time.h>

#include <map>
#include <string>
#include <vector>
#include <utility>

#include "arch/address.hpp"
#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/auth/user.hpp"
#include "clustering/administration/auth/username.hpp"
#include "clustering/administration/tables/database_metadata.hpp"
#include "containers/optional.hpp"
#include "logger.hpp"
#include "rpc/connectivity/peer_id.hpp"
#include "rpc/connectivity/server_id.hpp"
#include "rpc/semilattice/joins/macros.hpp"
#include "rpc/semilattice/joins/versioned.hpp"
#include "rpc/serialize_macros.hpp"
#include "time.hpp"

class auth_semilattice_metadata_t {
public:
    // For deserialization only
    auth_semilattice_metadata_t() { }

    explicit auth_semilattice_metadata_t(const std::string &initial_password)
        : m_users({create_initial_admin_pair(initial_password)}) { }

    static std::pair<auth::username_t, versioned_t<optional<auth::user_t>>>
        create_initial_admin_pair(const std::string &initial_password) {
        // Generate a timestamp that's minus our current time, so that the oldest
        // initial password wins. Unless the initial password is empty, which
        // should always lose.
        time_t version_ts = std::numeric_limits<time_t>::min();
        if (!initial_password.empty()) {
            time_t current_time = time(nullptr);
            if (current_time > 0) {
                version_ts = -current_time;
            } else {
                logWRN("The system time seems to be incorrectly set. Metadata "
                       "versioning will behave unexpectedly.");
            }
        }
        // Use a single iteration for better efficiency when starting out with an empty
        // password.
        uint32_t iterations = initial_password.empty()
                              ? 1
                              : auth::password_t::default_iteration_count;
        auth::password_t pw(initial_password, iterations);
        return std::make_pair(
            auth::username_t("admin"),
            versioned_t<optional<auth::user_t>>::make_with_manual_timestamp(
                version_ts,
                make_optional(auth::user_t(std::move(pw)))));
    }

    std::map<auth::username_t, versioned_t<optional<auth::user_t>>> m_users;
};

RDB_DECLARE_SERIALIZABLE(auth_semilattice_metadata_t);
RDB_DECLARE_SEMILATTICE_JOINABLE(auth_semilattice_metadata_t);
RDB_DECLARE_EQUALITY_COMPARABLE(auth_semilattice_metadata_t);

enum cluster_directory_peer_type_t {
    SERVER_PEER,
    PROXY_PEER
};

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(cluster_directory_peer_type_t, int8_t, SERVER_PEER, PROXY_PEER);

/* `proc_directory_metadata_t` is a part of `cluster_directory_metadata_t` that doesn't
change after the server starts and is only used for displaying in `server_status`. The
reason it's in a separate structure is to keep `cluster_directory_metadata_t` from being
too complicated. */
class proc_directory_metadata_t {
public:
    std::string version;   /* server version string, e.g. "rethinkdb 1.X.Y ..." */
    microtime_t time_started;
    int64_t pid;   /* really a `pid_t`, but we need a platform-independent type */
    std::string hostname;
    uint16_t reql_port;
    optional<uint16_t> http_admin_port;
    std::vector<std::string> argv;
};

RDB_DECLARE_SERIALIZABLE(proc_directory_metadata_t);

class cluster_directory_metadata_t {
public:
    cluster_directory_metadata_t() { }
    cluster_directory_metadata_t(
            server_id_t _server_id,
            peer_id_t _peer_id,
            const proc_directory_metadata_t &_proc,
            uint64_t _actual_cache_size_bytes,
            cluster_directory_peer_type_t _peer_type) :
        server_id(_server_id),
        peer_id(_peer_id),
        proc(_proc),
        actual_cache_size_bytes(_actual_cache_size_bytes),
        peer_type(_peer_type) { }
    /* Move constructor */
    cluster_directory_metadata_t(const cluster_directory_metadata_t &) = default;
    cluster_directory_metadata_t &operator=(const cluster_directory_metadata_t &)
        = default;

    server_id_t server_id;

    /* this is redundant, since a directory entry is always associated with a peer */
    peer_id_t peer_id;

    /* This group of fields are for showing in `rethinkdb.server_status` */
    proc_directory_metadata_t proc;
    uint64_t actual_cache_size_bytes;   /* might be user-set or automatically picked */

    cluster_directory_peer_type_t peer_type;
};

RDB_DECLARE_SERIALIZABLE_FOR_CLUSTER(cluster_directory_metadata_t);

template<class IdType, class T>
bool search_metadata_by_uuid(
        std::map<IdType, deletable_t<T> > *map,
        const IdType &uuid,
        typename std::map<IdType, deletable_t<T> >::iterator *it_out) {
    auto it = map->find(uuid);
    if (it != map->end() && !it->second.is_deleted()) {
        *it_out = it;
        return true;
    } else {
        return false;
    }
}

template<class T>
bool search_const_metadata_by_uuid(
        const std::map<uuid_u, deletable_t<T> > *map,
        const uuid_u &uuid,
        typename std::map<uuid_u, deletable_t<T> >::const_iterator *it_out) {
    auto it = map->find(uuid);
    if (it != map->end() && !it->second.is_deleted()) {
        *it_out = it;
        return true;
    } else {
        return false;
    }
}

bool search_db_metadata_by_name(
        const databases_semilattice_metadata_t &metadata,
        const name_string_t &name,
        database_id_t *id_out,
        admin_err_t *error_out);
admin_err_t db_not_found_error(const name_string_t &name);

#endif  // CLUSTERING_ADMINISTRATION_METADATA_HPP_
