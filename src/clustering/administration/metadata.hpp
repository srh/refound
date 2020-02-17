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
#include "containers/optional.hpp"
#include "logger.hpp"
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

admin_err_t db_not_found_error(const name_string_t &name);

#endif  // CLUSTERING_ADMINISTRATION_METADATA_HPP_
