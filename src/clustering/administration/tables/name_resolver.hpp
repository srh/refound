// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_TABLES_NAME_RESOLVER_HPP_
#define CLUSTERING_ADMINISTRATION_TABLES_NAME_RESOLVER_HPP_

#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/metadata.hpp"
#include "containers/lifetime.hpp"
#include "containers/name_string.hpp"

class table_meta_client_t;

template <typename T>
class resolved_id_optional_t {
public:
    struct no_such_name_t { };
    struct ambiguous_name_t { };

    explicit resolved_id_optional_t(T const &value)
        : m_optional(value),
          m_error(error_t::OK) { }
    explicit resolved_id_optional_t(T &&value)
        : m_optional(std::forward<T>(value)),
          m_error(error_t::OK) { }
    explicit resolved_id_optional_t(no_such_name_t)
        : m_optional(r_nullopt),
          m_error(error_t::NO_SUCH_NAME) { }
    explicit resolved_id_optional_t(ambiguous_name_t)
        : m_optional(r_nullopt),
          m_error(error_t::AMBIGUOUS_NAME) { }

    explicit operator bool() const noexcept {
        return m_error == error_t::OK;
    }

    T const &get() const {
        return m_optional.get();
    }

    bool is_no_such() const noexcept {
        return m_error == error_t::NO_SUCH_NAME;
    }

    bool is_ambiguous() const noexcept {
        return m_error == error_t::AMBIGUOUS_NAME;
    }

private:
    optional<T> m_optional;
    enum class error_t { OK = 0, NO_SUCH_NAME, AMBIGUOUS_NAME };
    error_t m_error;
};

class name_resolver_t {
public:
    name_resolver_t(
            std::shared_ptr<semilattice_read_view_t<cluster_semilattice_metadata_t>>,
            table_meta_client_t *,
            lifetime_t<artificial_reql_cluster_interface_t const &>);

    cluster_semilattice_metadata_t get_cluster_metadata() const noexcept;

    // A number of these functions come in two flavours, one where it fetches the
    // metadata itself and another where you can pass it in yourself. If you're doing
    // multiple operations in a row it's more efficient to get a copy of the metadata
    // yourself using the function above and reusing it.

    optional<name_string_t> database_id_to_name(
            database_id_t const &) const noexcept;
    optional<name_string_t> database_id_to_name(
            database_id_t const &,
            cluster_semilattice_metadata_t const &) const noexcept;

    optional<table_basic_config_t> table_id_to_basic_config(
            namespace_id_t const &,
            optional<database_id_t> const & = r_nullopt) const noexcept;

    resolved_id_optional_t<database_id_t> database_name_to_id(
            name_string_t const &) const noexcept;
    resolved_id_optional_t<database_id_t> database_name_to_id(
            name_string_t const &,
            cluster_semilattice_metadata_t const &) const noexcept;

    resolved_id_optional_t<namespace_id_t> table_name_to_id(
            database_id_t const &,
            name_string_t const &) const noexcept;

private:
    std::shared_ptr<semilattice_read_view_t<cluster_semilattice_metadata_t>>
        m_cluster_semilattice_view;
    table_meta_client_t *m_table_meta_client;
    artificial_reql_cluster_interface_t const &m_artificial_reql_cluster_interface;
};

#endif  // CLUSTERING_ADMINISTRATION_TABLES_NAME_RESOLVER_HPP_
