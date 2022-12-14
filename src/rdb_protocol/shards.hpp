// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_SHARDS_HPP_
#define RDB_PROTOCOL_SHARDS_HPP_

#include <algorithm>
#include <limits>
#include <map>
#include <utility>
#include <vector>

#include "arch/runtime/coroutines.hpp"
#include "btree/key_edges.hpp"
#include "btree/keys.hpp"
#include "btree/types.hpp"
#include "containers/archive/stl_types.hpp"
#include "containers/archive/varint.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/batching.hpp"
#include "rdb_protocol/configured_limits.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/datum_utils.hpp"
#include "rdb_protocol/profile.hpp"
#include "rdb_protocol/serialize_datum.hpp"
#include "rdb_protocol/wire_func.hpp"
#include "stl_utils.hpp"

enum class is_primary_t { NO, YES };

enum class require_sindexes_t { NO, YES};

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(require_sindexes_t,
                                      int8_t,
                                      require_sindexes_t::NO,
                                      require_sindexes_t::YES);

namespace ql {

template<class T>
T groups_to_batch(std::map<datum_t, T, optional_datum_less_t> *g) {
    if (g->size() == 0) {
        return T();
    } else {
        r_sanity_check(g->size() == 1 && !g->begin()->first.has());
        return std::move(g->begin()->second);
    }
}

// This stuff previously resided in the protocol, but has been broken out since
// we want to use this logic in multiple places.
typedef std::vector<ql::datum_t> datums_t;
typedef std::map<ql::datum_t, datums_t, optional_datum_less_t> groups_t;

struct rget_item_t {
    rget_item_t() = default;
    rget_item_t(store_key_t _key,
                ql::datum_t _sindex_key,
                ql::datum_t _data)
        : key(std::move(_key)),
          sindex_key(std::move(_sindex_key)),
          data(std::move(_data)) { }
    store_key_t key;
    ql::datum_t sindex_key, data;
};
RDB_DECLARE_SERIALIZABLE(rget_item_t);

// `sindex_compare_t` may block if there are a large number of things being compared.
class sindex_compare_t {
public:
    explicit sindex_compare_t(sorting_t _sorting)
        : sorting(_sorting), iterations_since_last_yield(0) { }
    bool operator()(const rget_item_t &l, const rget_item_t &r) {
        r_sanity_check(l.sindex_key.has() && r.sindex_key.has());

        ++iterations_since_last_yield;
        const size_t YIELD_INTERVAL = 10000;
        if (iterations_since_last_yield % YIELD_INTERVAL == 0) {
            coro_t::yield();
        }

        int cmp = l.sindex_key.cmp(r.sindex_key);
        if (cmp == 0) {
            return reversed(sorting)
                ? datum_t::extract_primary(l.key) > datum_t::extract_primary(r.key)
                : datum_t::extract_primary(l.key) < datum_t::extract_primary(r.key);
        } else {
            return reversed(sorting)
                ? cmp > 0
                : cmp < 0;
        }
    }
private:
    sorting_t sorting;
    size_t iterations_since_last_yield;
};

void debug_print(printf_buffer_t *, const rget_item_t &);

struct limit_read_last_key {
    limit_read_last_key() = default;
    explicit limit_read_last_key(const store_key_t &k) : is_decremented(false), raw_key(k) {}
    explicit limit_read_last_key(store_key_t &&k) : is_decremented(false), raw_key(std::move(k)) {}
    explicit limit_read_last_key(key_or_max &&k) : is_decremented(false), raw_key(std::move(k)) {}
    static limit_read_last_key min() {
        return limit_read_last_key();
    }
    static limit_read_last_key infinity() {
        limit_read_last_key ret;
        ret.is_decremented = false;
        ret.raw_key = key_or_max::infinity();
        return ret;
    }
    // It's disallowed to have is_decremented && raw_key.infinite.
    bool is_decremented = false;
    key_or_max raw_key;

    // Has precondition that raw_key not be infinite, i.e. is_max_key() is false.
    store_key_t successor_key() const {
        rassert(!raw_key.infinite);
        store_key_t ret = raw_key.key;
        if (!is_decremented) {
            ret.increment1();
        }
        return ret;
    }

    void set_to_key(const store_key_t &key) {
        is_decremented = false;
        raw_key = key_or_max(key);
    }

    bool is_max_key() const {
        return raw_key.infinite;
    }

    bool is_min_key() const {
        // This is just saying get_key() == "", we have this complication just
        // to obliviously maintain identical behavior (for now).
        return !raw_key.infinite && (raw_key.key.size() == 0 ||
            (is_decremented && raw_key.key.str().length() == 1 && raw_key.key.str()[0] == '\0'));
    }

    bool less_than_key(const store_key_t &rhs) const {
        return is_decremented ? (raw_key.lequal_to_key(rhs)) : raw_key.less_than_key(rhs);
    }

    bool greater_than_key(const store_key_t &rhs) const {
        if (!is_decremented) {
            return raw_key.greater_than_key(rhs);
        }
        if (raw_key.lequal_to_key(rhs)) {
            return false;
        }
        // TODO: Performance.
        store_key_t tmp = rhs;
        // OK if rhs is max key -- we return false correctly.
        tmp.increment1();
        return raw_key.greater_than_key(tmp);
    }
};

typedef std::vector<rget_item_t> raw_stream_t;
struct keyed_stream_t {
    raw_stream_t stream;
    limit_read_last_key last_key;
};
struct stream_t {
    // When we first construct a `stream_t`, it's always for a single shard.
    stream_t(region_t region, limit_read_last_key last_key)
        : substreams{{
            std::move(region),
                keyed_stream_t{raw_stream_t(), std::move(last_key)}}} { }
    stream_t() { }
    optional<std::pair<region_t, keyed_stream_t>> substreams;
};

class optimizer_t {
public:
    optimizer_t();
    optimizer_t(const datum_t &_row,
                const datum_t &_val);

    void swap_if_other_better(optimizer_t *other,
                              bool (*beats)(const datum_t &val1, const datum_t &val2));
    datum_t unpack(const char *name);
    datum_t row, val;
};

template <cluster_version_t W>
void serialize_grouped(write_message_t *wm, const optimizer_t &o) {
    serialize<W>(wm, o.row.has());
    if (o.row.has()) {
        r_sanity_check(o.val.has());
        serialize<W>(wm, o.row);
        serialize<W>(wm, o.val);
    }
}
template <cluster_version_t W>
archive_result_t deserialize_grouped(read_stream_t *s, optimizer_t *o) {
    archive_result_t res;
    bool has;
    res = deserialize<W>(s, &has);
    if (bad(res)) { return res; }
    if (has) {
        res = deserialize<W>(s, &o->row);
        if (bad(res)) { return res; }
        res = deserialize<W>(s, &o->val);
        if (bad(res)) { return res; }
    }
    return archive_result_t::SUCCESS;
}

// We write all of these serializations and deserializations explicitly because:
// * It stops people from inadvertently using a new `grouped_t<T>` without thinking.
// * Some grouped elements need specialized serialization.
template <cluster_version_t W>
void serialize_grouped(
    write_message_t *wm, const datum_t &d) {
    serialize<W>(wm, d.has());
    if (d.has()) {
        serialize<W>(wm, d);
    }
}
template <cluster_version_t W>
void serialize_grouped(write_message_t *wm, uint64_t sz) {
    serialize_varint_uint64(wm, sz);
}
template <cluster_version_t W>
void serialize_grouped(write_message_t *wm, double d) {
    serialize<W>(wm, d);
}
template <cluster_version_t W>
void serialize_grouped(write_message_t *wm,
                       const std::pair<double, uint64_t> &p) {
    serialize<W>(wm, p.first);
    serialize_varint_uint64(wm, p.second);
}
template <cluster_version_t W>
void serialize_grouped(write_message_t *wm, const stream_t &sz) {
    serialize<W>(wm, sz);
}
template <cluster_version_t W>
void serialize_grouped(write_message_t *wm, const datums_t &ds) {
    serialize<W>(wm, ds);
}

template <cluster_version_t W>
archive_result_t deserialize_grouped(
    read_stream_t *s, datum_t *d) {
    bool has;
    archive_result_t res = deserialize<W>(s, &has);
    if (bad(res)) { return res; }
    if (has) {
        return deserialize<W>(s, d);
    } else {
        d->reset();
        return archive_result_t::SUCCESS;
    }
}
template <cluster_version_t W>
archive_result_t deserialize_grouped(read_stream_t *s, uint64_t *sz) {
    return deserialize_varint_uint64(s, sz);
}
template <cluster_version_t W>
archive_result_t deserialize_grouped(read_stream_t *s, double *d) {
    return deserialize<W>(s, d);
}
template <cluster_version_t W>
archive_result_t deserialize_grouped(read_stream_t *s,
                                     std::pair<double, uint64_t> *p) {
    archive_result_t res = deserialize<W>(s, &p->first);
    if (bad(res)) { return res; }
    return deserialize_varint_uint64(s, &p->second);
}
template <cluster_version_t W>
archive_result_t deserialize_grouped(read_stream_t *s, stream_t *sz) {
    return deserialize<W>(s, sz);
}
template <cluster_version_t W>
archive_result_t deserialize_grouped(read_stream_t *s, datums_t *ds) {
    return deserialize<W>(s, ds);
}

// This is basically a templated typedef with special serialization.
template<class T>
class grouped_t {
public:
    // We assume > v1_13 ordering.  We could get fancy and allow any
    // ordering, but usage of grouped_t inside of secondary index functions is the
    // only place where we'd want v1_13 ordering, so let's not bother.
    grouped_t() : m(optional_datum_less_t()) { }
    virtual ~grouped_t() { } // See grouped_data_t below.
    template <cluster_version_t W>
    friend
    typename std::enable_if<W == cluster_version_t::CLUSTER, void>::type
    serialize(write_message_t *wm, const grouped_t &g) {
        serialize_varint_uint64(wm, g.m.size());
        for (auto it = g.m.begin(); it != g.m.end(); ++it) {
            serialize_grouped<W>(wm, it->first);
            serialize_grouped<W>(wm, it->second);
        }
    }
    template <cluster_version_t W>
    friend
    typename std::enable_if<W == cluster_version_t::CLUSTER, archive_result_t>::type
    deserialize(read_stream_t *s, grouped_t *g) {
        guarantee(g->m.empty());

        uint64_t sz;
        archive_result_t res = deserialize_varint_uint64(s, &sz);
        if (bad(res)) { return res; }
        if (sz > std::numeric_limits<size_t>::max()) {
            return archive_result_t::RANGE_ERROR;
        }
        auto pos = g->m.begin();
        for (uint64_t i = 0; i < sz; ++i) {
            std::pair<datum_t, T> el;
            res = deserialize_grouped<W>(s, &el.first);
            if (bad(res)) { return res; }
            res = deserialize_grouped<W>(s, &el.second);
            if (bad(res)) { return res; }
            pos = g->m.insert(pos, std::move(el));
        }
        return archive_result_t::SUCCESS;
    }

    // You're not allowed to use, in any way, the intrinsic ordering of the
    // grouped_t.  If you're processing its data into a parallel map, you're ok,
    // since the parallel map provides its own ordering (that you specify).  This
    // way, we know it's OK for the map ordering to use any reql_version (instead of
    // taking that as a parameter, which would be completely impracticable).
    typename std::map<datum_t, T, optional_datum_less_t>::iterator begin() {
        return m.begin();
    }
    typename std::map<datum_t, T, optional_datum_less_t>::iterator end() {
        return m.end();
    }

    std::pair<typename std::map<datum_t, T, optional_datum_less_t>::iterator, bool>
    insert(std::pair<datum_t, T> &&val) {
        return m.insert(std::move(val));
    }
    void
    erase(typename std::map<datum_t, T, optional_datum_less_t>::iterator pos) {
        m.erase(pos);
    }

    size_t size() { return m.size(); }
    void clear() { return m.clear(); }
    T &operator[](const datum_t &k) { return m[k]; }

    void swap(grouped_t<T> &other) { m.swap(other.m); }
    std::map<datum_t, T, optional_datum_less_t> *get_underlying_map() {
        return &m;
    }

    const std::map<datum_t, T, optional_datum_less_t> *get_underlying_map() const {
        return &m;
    }

private:
    std::map<datum_t, T, optional_datum_less_t> m;
};

void debug_print(printf_buffer_t *buf, const keyed_stream_t &stream);
void debug_print(printf_buffer_t *buf, const stream_t &stream);

template <class T>
void debug_print(printf_buffer_t *buf, const grouped_t<T> &value) {
    buf->appendf("grouped_t(");
    debug_print(buf, *value.get_underlying_map());
    buf->appendf(")");
}

namespace grouped_details {

template <class T>
class grouped_pair_compare_t {
public:
    explicit grouped_pair_compare_t(reql_version_t _reql_version)
        : reql_version(_reql_version) { }

    bool operator()(const std::pair<datum_t, T> &a,
                    const std::pair<datum_t, T> &b) const {
        // We know the keys are different, this is only used in
        // iterate_ordered_by_version.
        return a.first.compare_lt(reql_version, b.first);
    }

private:
    reql_version_t reql_version;
};

}  // namespace grouped_details

// We need a separate class for this because inheriting from
// `slow_atomic_countable_t` deletes our copy constructor, but boost variants
// want us to have a copy constructor.
class grouped_data_t : public grouped_t<datum_t>,
                       public slow_atomic_countable_t<grouped_data_t> { };

typedef boost::variant<
    grouped_t<uint64_t>, // Count.
    grouped_t<double>, // Sum.
    grouped_t<std::pair<double, uint64_t> >, // Avg.
    grouped_t<ql::datum_t>, // Reduce (may be NULL)
    grouped_t<optimizer_t>, // min, max
    grouped_t<stream_t>, // No terminal.
    exc_t // Don't re-order (we don't want this to initialize to an error.)
    > result_t;

typedef boost::variant<map_wire_func_t,
                       group_wire_func_t,
                       filter_wire_func_t,
                       concatmap_wire_func_t,
                       distinct_wire_func_t,
                       zip_wire_func_t
                       > transform_variant_t;

class op_t {
public:
    op_t() { }
    virtual ~op_t() { }
    virtual void apply_op(env_t *env,
                            groups_t *groups,
                            // Returns a datum that might be null
                            const std::function<datum_t()> &lazy_sindex_val) = 0;
};

struct limit_read_t {
    is_primary_t is_primary;
    size_t n;
    region_t shard;
    limit_read_last_key last_key;
    sorting_t sorting;
    std::vector<scoped_ptr_t<op_t> > *ops;
};


typedef boost::variant<count_wire_func_t,
                       sum_wire_func_t,
                       avg_wire_func_t,
                       min_wire_func_t,
                       max_wire_func_t,
                       reduce_wire_func_t,
                       limit_read_t
                       > terminal_variant_t;

class accumulator_t {
public:
    accumulator_t();
    virtual ~accumulator_t();
    // May be overridden as an optimization (currently is for `count`).
    virtual bool uses_val() { return true; }
    virtual void stop_at_boundary(const limit_read_last_key &) { }
    virtual bool should_send_batch() = 0;
    virtual continue_bool_t apply_accumulator(
            env_t *env,
            groups_t *groups,
            const store_key_t &key,
            // Returns a datum that might be null
            const std::function<datum_t()> &lazy_sindex_val) = 0;
    void finish(continue_bool_t last_cb, result_t *out);
protected:
    void mark_finished();
private:
    virtual void finish_impl(continue_bool_t last_cb, result_t *out) = 0;
    bool finished;
};

class eager_acc_t {
public:
    eager_acc_t() { }
    virtual ~eager_acc_t() { }
    virtual void apply_eager(env_t *env, groups_t *groups) = 0;
    virtual void add_res(env_t *env, result_t *res, sorting_t sorting) = 0;
    virtual scoped_ptr_t<val_t> finish_eager(
        backtrace_id_t bt, bool is_grouped,
        const ql::configured_limits_t &limits) = 0;
};

scoped_ptr_t<accumulator_t> make_append(limit_read_last_key last_key,
                                        sorting_t sorting,
                                        batcher_t *batcher,
                                        require_sindexes_t require_sindex_val);
scoped_ptr_t<accumulator_t> make_terminal(const terminal_variant_t &t);
scoped_ptr_t<eager_acc_t> make_to_array();
scoped_ptr_t<eager_acc_t> make_eager_terminal(const terminal_variant_t &t);
scoped_ptr_t<op_t> make_op(const transform_variant_t &tv);

} // namespace ql

#endif  // RDB_PROTOCOL_SHARDS_HPP_
