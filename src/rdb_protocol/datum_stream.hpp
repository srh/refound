// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_DATUM_STREAM_HPP_
#define RDB_PROTOCOL_DATUM_STREAM_HPP_

#include <algorithm>
#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "btree/key_edges.hpp"
#include "containers/optional.hpp"
#include "containers/scoped.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/protocol.hpp"

namespace ql {

class env_t;
class scope_env_t;
class func_t;
class response_t;
class val_t;

enum class feed_type_t { not_feed, point, stream, orderby_limit, unioned };

// Handle unions of changefeeds; if there's no plausible unioned type then we
// just return `feed_type_t::unioned`.
inline feed_type_t union_of(feed_type_t a, feed_type_t b) {
    switch (a) {
    case feed_type_t::stream: // fallthru
    case feed_type_t::point: // fallthru
    case feed_type_t::orderby_limit:
        return (b == a || b == feed_type_t::not_feed) ? a : feed_type_t::unioned;
    case feed_type_t::unioned: return a;
    case feed_type_t::not_feed: return b;
    default: unreachable();
    }
    unreachable();
}

#if RDB_CF
struct active_state_t {
    std::map<uuid_u, std::pair<key_range_t, uint64_t> > shard_last_read_stamps;
    DEBUG_ONLY(optional<std::string> sindex;)
};
#endif  // RDB_CF

#if RDB_CF
struct changespec_t {
    changespec_t(changefeed::keyspec_t _keyspec,
                 scoped<datum_stream_t> &&_stream)
        : keyspec(std::move(_keyspec)),
          stream(std::move(_stream)) { }
    changefeed::keyspec_t keyspec;
    scoped<datum_stream_t> stream;
};
#endif  // RDB_CF

class datum_stream_t : public bt_rcheckable_t {
public:
    virtual ~datum_stream_t() { }
    virtual void set_notes(response_t *) const { }

#if RDB_CF
    virtual std::vector<changespec_t> get_changespecs() = 0;
#endif
    virtual void add_transformation(transform_variant_t &&tv, backtrace_id_t bt) = 0;
#if RDB_CF
    virtual bool add_stamp(changefeed_stamp_t stamp);
    virtual optional<active_state_t> get_active_state();
#endif  // RDB_CF
    void add_grouping(transform_variant_t &&tv,
                      backtrace_id_t bt);

    scoped_ptr_t<val_t> run_terminal(env_t *env, const terminal_variant_t &tv);
    scoped_ptr_t<val_t> to_array(env_t *env);

    // Returns false or NULL respectively if stream is lazy.
    virtual bool is_array() const = 0;
    virtual datum_t as_array(env_t *env) = 0;

    bool is_grouped() const { return grouped; }

    // Gets the next elements from the stream.  (Returns zero elements only when
    // the end of the stream has been reached.  Otherwise, returns at least one
    // element.)  (Wrapper around `next_batch_impl`.)
    std::vector<datum_t>
    next_batch(env_t *env, const batchspec_t &batchspec);
    // Prefer `next_batch`.  Cannot be used in conjunction with `next_batch`.
    virtual datum_t next(env_t *env, const batchspec_t &batchspec);
    virtual bool is_exhausted() const = 0;
    virtual feed_type_t cfeed_type() const = 0;
    virtual bool is_infinite() const = 0;

    virtual void accumulate(
        env_t *env, eager_acc_t *acc, const terminal_variant_t &tv) = 0;
    virtual void accumulate_all(env_t *env, eager_acc_t *acc) = 0;

protected:
    bool batch_cache_exhausted() const;
    void check_not_grouped(const char *msg);
    explicit datum_stream_t(backtrace_id_t bt);

private:
    virtual std::vector<datum_t>
    next_batch_impl(env_t *env, const batchspec_t &batchspec) = 0;

    std::vector<datum_t> batch_cache;
    size_t batch_cache_index;
    bool grouped;
};

class eager_datum_stream_t : public datum_stream_t {
protected:
    explicit eager_datum_stream_t(backtrace_id_t _bt)
        : datum_stream_t(_bt) { }
    virtual datum_t as_array(env_t *env);
    bool ops_to_do() { return ops.size() != 0; }

protected:
#if RDB_CF
    virtual std::vector<changespec_t> get_changespecs() {
        rfail(base_exc_t::LOGIC, "%s", "Cannot call `changes` on an eager stream.");
    }
#endif  // RDB_CF
    std::vector<transform_variant_t> transforms;

    virtual void add_transformation(transform_variant_t &&tv,
                                    backtrace_id_t bt);

private:
    enum class done_t { YES, NO };

    virtual bool is_array() const = 0;

    virtual void accumulate(env_t *env, eager_acc_t *acc, const terminal_variant_t &tv);
    virtual void accumulate_all(env_t *env, eager_acc_t *acc);

    done_t next_grouped_batch(env_t *env, const batchspec_t &bs, groups_t *out);
    virtual std::vector<datum_t>
    next_batch_impl(env_t *env, const batchspec_t &bs);
    virtual std::vector<datum_t>
    next_raw_batch(env_t *env, const batchspec_t &bs) = 0;

    std::vector<scoped_ptr_t<op_t> > ops;
};

class wrapper_datum_stream_t : public eager_datum_stream_t {
protected:
    explicit wrapper_datum_stream_t(scoped<datum_stream_t> &&_source)
        : eager_datum_stream_t(_source->backtrace()),
          source(std::move(_source)) { }
private:
    virtual bool is_array() const { return source->is_array(); }
    virtual datum_t as_array(env_t *env) {
        return source->is_array() && !source->is_grouped()
            ? eager_datum_stream_t::as_array(env)
            : datum_t();
    }
    virtual bool is_exhausted() const {
        return source->is_exhausted() && batch_cache_exhausted();
    }
    virtual feed_type_t cfeed_type() const {
        return source->cfeed_type();
    }
    virtual bool is_infinite() const {
        return source->is_infinite();
    }

protected:
    const scoped<datum_stream_t> source;
};

// SATURATED can't happen anymore (because we don't actually shard).
//
// Every shard is in a particular state.  ACTIVE means we should read more data
// from it, SATURATED means that there's more data to read but we didn't
// actually use any of the data we read last time so don't bother issuing
// another read, and `EXHAUSTED` means there's no more to read.  (Generally when
// iterating over a stream ordered by the primary key, range shards we've
// already read will be EXHAUSTED, the range shard we're currently reading will
// be ACTIVE, and the range shards we've yet to read will be SATURATED.)
//
// We track these on a per-hash-shard basis because it's easier, but we decide
// whether or not to issue reads on a per-range-shard basis.
// enum class range_state_t { ACTIVE, EXHAUSTED };

// void debug_print(printf_buffer_t *buf, const range_state_t &rs);

// This type no longer even has a cache (because we consume everything "fresh" because we
// don't unshard anymore).
struct active_range_with_cache {
    // This is the range of values that we have yet to read from the shard.  We
    // store a range instead of just a `store_key_t` because this range is only
    // a restriction of the indexed range for primary key reads.
    lower_key_bound_range key_range;
    // This no longer has a "cache".  Is this type getting obsolete?

    // No data in the cache and nothing to read from the shards.
    bool totally_exhausted() const {
        return key_range.is_empty();
    }
};
void debug_print(printf_buffer_t *buf, const active_range_with_cache &hrwc);

struct active_ranges_t {
    optional<std::pair<key_range_t, active_range_with_cache>> ranges;
    bool totally_exhausted() const;
};
void debug_print(printf_buffer_t *buf, const active_ranges_t &ar);

} // namespace ql

#endif // RDB_PROTOCOL_DATUM_STREAM_HPP_
