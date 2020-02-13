// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_ARTIFICIAL_TABLE_BACKEND_HPP_
#define RDB_PROTOCOL_ARTIFICIAL_TABLE_BACKEND_HPP_

#include <string>
#include <vector>

#include "concurrency/cross_thread_mutex.hpp"
#include "containers/name_string.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/datum.hpp"

namespace auth {
class user_context_t;
}

namespace ql {
class datum_stream_t;
class datumspec_t;
namespace changefeed { class streamspec_t; }
}

struct admin_err_t;
enum class sorting_t;

/* `artificial_table_backend_t` is the interface that `artificial_table_t` uses to access
the actual data or configuration. There is one subclass for each table like
`rethinkdb.table_config`, `rethinkdb.table_status`, and so on. */

// QQQ: Remove this.
class artificial_table_backend_t : public home_thread_mixin_t {
public:
    static namespace_id_t compute_artificial_table_id(const name_string_t &name);

    /* Notes:
     1. `read_all_rows_as_*()`, `read_row()`, and `write_row()` all return `false` and
        set `*error_out` if an error occurs. Note that if a row is absent in
        `read_row()`, this doesn't count as an error.
     2. If `write_row()` is called concurrently with `read_row()` or
        `read_all_rows_as_*()`, it is undefined whether the read will see the write or
        not.
     3. `get_primary_key_name()`, `read_all_rows_as_*()`, `read_row()` and `write_row()`
        can be called on any thread. */

    explicit artificial_table_backend_t(name_string_t const &table_name);
    virtual ~artificial_table_backend_t();

    name_string_t const &get_table_name() const;
    namespace_id_t const &get_table_id() const;

    /* Returns the name of the primary key for the table. The return value must not
    change. This must not block. */
    virtual std::string get_primary_key_name() = 0;

    // Returns the full dataset in a vector (in `rows_out`) after applying the filtering
    // and sorting specified by `datumspec` and `sorting`.
    bool read_all_rows_filtered(
        auth::user_context_t const &user_context,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out);

    /* `read_all_rows_filtered_as_stream()` returns the full dataset as a stream (using
       read_all_rows_as_vector) and applies the applicable filtering and sorting (as
       specified in `datumspec` and `sorting`). */
    bool read_all_rows_filtered_as_stream(
        auth::user_context_t const &user_context,
        ql::backtrace_id_t bt,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        const signal_t *interruptor,
        counted_t<ql::datum_stream_t> *rows_out,
        admin_err_t *error_out);

    /* Sets `*row_out` to the current value of the row, or an empty `datum_t` if no such
    row exists. */
    virtual bool read_row(
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        admin_err_t *error_out) = 0;

    /* Called when the user issues a write command on the row. Calling `write_row()` on a
    row that doesn't exist means an insertion; calling `write_row` with
    `*new_value_inout` an empty `datum_t` means a deletion. `pkey_was_autogenerated` will
    be set to `true` only if `primary_key` is a newly-generated UUID created for the
    purpose of this insert. If the backend makes additional changes to the row before
    inserting it (such as filling in omitted fields) then it can write to
    `*new_value_inout`, but it cannot change an empty datum to a non-empty datum or vice
    versa. */
    virtual bool write_row(
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor,
        admin_err_t *error_out) = 0;

#if RDB_CF
    virtual bool read_changes(
        ql::env_t *env,
        const ql::changefeed::streamspec_t &ss,
        ql::backtrace_id_t bt,
        const signal_t *interruptor,
        counted_t<ql::datum_stream_t> *cfeed_out,
        admin_err_t *error_out) = 0;
#endif

    cross_thread_mutex_t::acq_t aquire_transaction_mutex() {
        return cross_thread_mutex_t::acq_t(&transaction_mutex);
    }

#if !RDB_CF
    // Declared here so we don't have to comment out every call from a destructor.
    void begin_changefeed_destruction() {}
#endif

    static const uuid_u base_table_id;

private:
    virtual bool read_all_rows_as_vector(
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out) = 0;

    name_string_t m_table_name;
    namespace_id_t m_table_id;
    cross_thread_mutex_t transaction_mutex;
};


/* `artificial_table_backend_t` is the interface that `artificial_table_t` uses to access
the actual data or configuration. There is one subclass for each table like
`rethinkdb.table_config`, `rethinkdb.table_status`, and so on. */

class artificial_table_fdb_backend_t : public home_thread_mixin_t {
public:
    static namespace_id_t compute_artificial_table_id(const name_string_t &name);

    /* Notes:
     1. `read_all_rows_as_*()`, `read_row()`, and `write_row()` all return `false` and
        set `*error_out` if an error occurs. Note that if a row is absent in
        `read_row()`, this doesn't count as an error.
     2. If `write_row()` is called concurrently with `read_row()` or
        `read_all_rows_as_*()`, it is undefined whether the read will see the write or
        not.
     3. `get_primary_key_name()`, `read_all_rows_as_*()`, `read_row()` and `write_row()`
        can be called on any thread. */

    explicit artificial_table_fdb_backend_t(name_string_t const &table_name);
    virtual ~artificial_table_fdb_backend_t();

    name_string_t const &get_table_name() const;
    namespace_id_t const &get_table_id() const;

    /* Returns the name of the primary key for the table. The return value must not
    change. This must not block. */
    virtual std::string get_primary_key_name() = 0;

    // Returns the full dataset in a vector (in `rows_out`) after applying the filtering
    // and sorting specified by `datumspec` and `sorting`.
    bool read_all_rows_filtered(
        auth::user_context_t const &user_context,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out);

    /* `read_all_rows_filtered_as_stream()` returns the full dataset as a stream (using
       read_all_rows_as_vector) and applies the applicable filtering and sorting (as
       specified in `datumspec` and `sorting`). */
    bool read_all_rows_filtered_as_stream(
        auth::user_context_t const &user_context,
        ql::backtrace_id_t bt,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        const signal_t *interruptor,
        counted_t<ql::datum_stream_t> *rows_out,
        admin_err_t *error_out);

    /* Sets `*row_out` to the current value of the row, or an empty `datum_t` if no such
    row exists. */
    virtual bool read_row(
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        admin_err_t *error_out) = 0;

    /* Called when the user issues a write command on the row. Calling `write_row()` on a
    row that doesn't exist means an insertion; calling `write_row` with
    `*new_value_inout` an empty `datum_t` means a deletion. `pkey_was_autogenerated` will
    be set to `true` only if `primary_key` is a newly-generated UUID created for the
    purpose of this insert. If the backend makes additional changes to the row before
    inserting it (such as filling in omitted fields) then it can write to
    `*new_value_inout`, but it cannot change an empty datum to a non-empty datum or vice
    versa. */
    virtual bool write_row(
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor,
        admin_err_t *error_out) = 0;

#if RDB_CF
    virtual bool read_changes(
        ql::env_t *env,
        const ql::changefeed::streamspec_t &ss,
        ql::backtrace_id_t bt,
        const signal_t *interruptor,
        counted_t<ql::datum_stream_t> *cfeed_out,
        admin_err_t *error_out) = 0;
#endif

    cross_thread_mutex_t::acq_t aquire_transaction_mutex() {
        return cross_thread_mutex_t::acq_t(&transaction_mutex);
    }

#if !RDB_CF
    // Declared here so we don't have to comment out every call from a destructor.
    void begin_changefeed_destruction() {}
#endif

    static const uuid_u base_table_id;

private:
    virtual bool read_all_rows_as_vector(
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out) = 0;

    name_string_t m_table_name;
    namespace_id_t m_table_id;
    cross_thread_mutex_t transaction_mutex;
};

#endif /* RDB_PROTOCOL_ARTIFICIAL_TABLE_BACKEND_HPP_ */

