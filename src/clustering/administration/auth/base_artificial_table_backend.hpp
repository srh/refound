// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_BASE_ARTIFICIAL_TABLE_BACKEND_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_BASE_ARTIFICIAL_TABLE_BACKEND_HPP

#include <memory>
#include <string>
#include <vector>

#include "containers/lifetime.hpp"
#include "rdb_protocol/artificial_table/caching_cfeed_backend.hpp"
#include "rpc/semilattice/view.hpp"

class cluster_semilattice_metadata_t;
class auth_semilattice_metadata_t;
template <class T> class lifetime_t;
class name_resolver_t;

namespace auth {

class base_artificial_table_backend_t :
#if RDB_CF
    public caching_cfeed_artificial_table_backend_t
#else
    public artificial_table_backend_t
#endif
{
public:
    base_artificial_table_backend_t(
            name_string_t const &table_name,
            rdb_context_t *rdb_context,
            RDB_CF_UNUSED lifetime_t<name_resolver_t const &> name_resolver,
            std::shared_ptr<semilattice_readwrite_view_t<auth_semilattice_metadata_t>>
                auth_semilattice_view,
            std::shared_ptr<semilattice_read_view_t<cluster_semilattice_metadata_t>>
                cluster_semilattice_view)
#if RDB_CF
        : caching_cfeed_artificial_table_backend_t(
            table_name, rdb_context, name_resolver),
#else
        : artificial_table_backend_t(
            table_name, rdb_context),
#endif
          m_auth_semilattice_view(std::move(auth_semilattice_view)),
#if RDB_CF
          m_auth_subscription([this](){ notify_all(); }, m_auth_semilattice_view),
#endif
          m_cluster_semilattice_view(std::move(cluster_semilattice_view)) {
    }

    ~base_artificial_table_backend_t() {
#if RDB_CF
        begin_changefeed_destruction();
#endif
    }

    std::string get_primary_key_name() {
        return "id";
    }

protected:
    std::shared_ptr<semilattice_readwrite_view_t<auth_semilattice_metadata_t>>
        m_auth_semilattice_view;
#if RDB_CF
    semilattice_read_view_t<auth_semilattice_metadata_t>::subscription_t
        m_auth_subscription;
#endif
    std::shared_ptr<semilattice_read_view_t<cluster_semilattice_metadata_t>>
        m_cluster_semilattice_view;
};

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_BASE_ARTIFICIAL_TABLE_BACKEND_HPP
