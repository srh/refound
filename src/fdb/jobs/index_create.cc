#include "fdb/jobs/index_create.hpp"

#include "clustering/tables/table_metadata.hpp"
#include "debug.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs/job_utils.hpp"
#include "fdb/system_tables.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/btree.hpp"  // For compute_keys

// #define icdbf(...) debugf(__VA_ARGS__)
#define icdbf(...)

/* We encounter a question of conflict resolution when building a secondary index.  The
   problem is, with a naive implementation:

1. When performing a table write (key W), we must read the jobstate info for
unindexed_lower_bound (called min_pkey here) and unindexed_upper_bound (called end_pkey
here).

2. Every sindex building operation requires that min_pkey be read from the jobstate --
suppose its value is named K1 -- and then min_pkey written, with some value named K2.  The
sindex building operation also reads all the pkey rows with values (K1, K2].

There are four primary key regions involved.  A. [-infinity, K1], B. (K1, K2], C. (K2,
end_pkey), D. [end_pkey, +infinity)

Our table write W, occurring at the same time as the sindex building operation, only knows
the value K1, because K2 is TBD.

- If W is in region A or D, our write will need to insert into the new index.

- If W is in region C (whatever that turns out to be), then our write will not need to
  insert into the new index.

- If W is in region B, then it will conflict with the index building operation.

Generally, a write operation in region B unavoidably causes a conflict.

(Why do we have region D and end_pkey at all?  So that if active table inserts are
increasing by value (e.g. with an incrementing primary key or timestamp primary key) at
the end of the table -- a common use case -- we'll finish building the index when we hit
end_pkey, instead of chasing the table inserts.  We have more mitigations of chasing inserts
described below.)

The basic technique we might use is as follows:

> If we just use normal transactions including table writes to region B, we would get such
> a conflict, because the table write causes a write to happen in region B, conflicting
> with the sindex building operation's read.  The sindex building operation would have to
> retry -- maybe it would be less ambitious about what a large chunk of keys it takes on
> the next attempt.  That would be okay.  But we get another conflict every time the
> sindex build transaction commits, because it updates the jobstate's min_pkey field, with
> _all_ write transactions ongoing at its commit time having to retry because they've read
> that field.

For now we go with a simple improvement: Have table write operations read the jobstate
with a snapshot read.  And have the sindex building operation set a conflict range on region B?

Wait, let me work this out...

Suppose txn S (for Sindex build) reads the jobstate, reads a key range (which turns out to
be B), and then writes the jobstate and adds sindex entries for those keys, and adds a
WRITE conflict range for those keys

And suppose txn T (for Table write) snapshot reads the jobstate, seeing min_pkey = K1, and has a key in:

 1.1. Region A, and T commits before S:

    - T writes the pkey and updates the index.

 1.2. Region A, and S commits before T:

    - T writes the pkey and updates the index.  No conflict on the jobstate because of
      the snapshot read.

 2.1. Region C, and T commits before S:

    - T writes the pkey and doesn't update the index.

 2.2. Region C, and S commits before T:

    - T writes the pkey and doesn't update the index.  No conflict on the jobstate
      because of the snapshot read.

 3.1. Region B, and T commits before S:

    - T writes the pkey and doesn't update the index.  The commit of S fails because its
      read conflicted with T's write.

 3.2. Region B, and S commits before T:

    - T writes the pkey and doesn't update the index.  The commit of S succeeds because
      it's first, and T fails because its read of the pkey in region B conflicts with the
      conflict range set by S.

      Note that if we used ordinary transactions, then T would fail because it read an out
      of date version of jobstate and based its behavior on that.

Now, what we'd like is for index building to win every conflict it has.  We want index
building to be able to make progress.  We _could_ give it the ability to "ask" write
transactions to help it update a certain key range, which is Region B.  Then incoming
table write operations, along with the index build operation, could all attempt to
complete that update themselves, if they touch Region B, in the same transaction.  One of
them will succeed, and the rest will get a conflict, just once.  The Sindex build
operation would still try smaller update intervals on its own attempts, reducing the size
of the "ask" interval, to avoid getting stuck on a too-large "ask" interval.  (For
example, suppose a million keys were written into the "ask" interval after it were
created.)

Another problem we face is that of chasing behind high-volume table inserts.  We mitigate
this at the end of the table simply with unindexed_upper_bound.

OOO: We can fail to ever create an index job in the first place, because we race on
inserts when scanning for unindexed_upper_bound.

But we can still end up having inserts serially increasing by primary key before the upper
bound.  One way to reduce (but not theoretically eliminate) this problem is to just flip
our index building logic to build the index in reverse order -- because usually, keys
increase.

Maybe we should do that.

*/

/* How things work:

Whenever we perform a write (to a table), we "read" the table config metadata --
discovering what sindexes there are, if there's a write hook, etc.  But we already have
it in-cache, we just check the config version.  So when building an index, we don't want
to update the table config more then twice: when index building begins, and when it
ends.  So the table config's sindex_config_t holds a shared task id, that's it, until
the index is done being built.

The information about the in-progress construction of the index changes frequently, so
it needs to be stored elsewhere.  It gets stored in index_jobstate_by_task.
*/

ql::datum_t parse_table_value(const char *value, size_t data_length) {
    buffer_read_stream_t stream(value, data_length);

    ql::datum_t ret;
    archive_result_t res = ql::datum_deserialize(&stream, &ret);
    guarantee(!bad(res), "table value misparsed");  // TODO: msg, graceful, etc.
    guarantee(size_t(stream.tell()) == data_length);  // TODO: msg, graceful, etc.
    return ret;
}

std::unordered_map<std::string, sindex_metaconfig_t>::iterator
find_sindex(std::unordered_map<std::string, sindex_metaconfig_t> *sindexes,
        const sindex_id_t &id) {
    auto it = sindexes->begin();
    for (; it != sindexes->end(); ++it) {
        if (it->second.sindex_id == id) {
            return it;
        }
    }
    return it;  // Returns sindexes->end().
}

// 0 if first read thus value unspecified -- this is the value we feed into FDB api
int recommended_target_bytes(const index_create_retry_state &state) {
    if (!state.last_bytes_read.has_value() || !state.last_key_count.has_value()) {
        return 0;
    }
    size_t val;
    if (*state.last_key_count <= 1) {
        val = (1 + *state.last_bytes_read) * 1.5;
    } else {
        val = std::max<size_t>(1, *state.last_bytes_read / 2);
    }
    return static_cast<int>(std::min<size_t>(INT_MAX, val));
}

job_execution_result execute_index_create_job(
        const signal_t *interruptor,
        FDBTransaction *txn,
        const fdb_job_info &info,
        const fdb_job_index_create &index_create_info,
        index_create_retry_state *retry_state) {
    UNUSED const uint64_t retry_count = retry_state->retry_count++;

    const bool reverse = true;
    icdbf("eicj %s\n", uuid_to_str(info.shared_task_id.value).c_str());
    // TODO: Maybe caller can pass clock (as in all jobs).

    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_value_fut<fdb_job_info> real_info_fut
        = transaction_get_real_job_info(txn, info);
    fdb_value_fut<table_config_t> table_config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, index_create_info.table_id);
    fdb_value_fut<reqlfdb_config_version> cv_fut
        = transaction_get_config_version(txn);

    fdb_value_fut<fdb_index_jobstate> jobstate_fut
        = transaction_lookup_uq_index<index_jobstate_by_task>(txn, info.shared_task_id);

    job_execution_result ret;
    if (!block_and_check_info(info, std::move(real_info_fut), interruptor)) {
        return ret;
    }

    fdb_index_jobstate jobstate;
    if (!jobstate_fut.block_and_deserialize(interruptor, &jobstate)) {
        ret.failed.set("jobstate in invalid state");
        return ret;
    }

    std::string pkey_prefix = rfdb::table_pkey_prefix(index_create_info.table_id);
    icdbf("eicj '%s'\n", debug_str(pkey_prefix).c_str());

    store_key_t js_lower_bound(jobstate.unindexed_lower_bound.ukey);
    optional<store_key_t> js_upper_bound
        = jobstate.unindexed_upper_bound.has_value()
        ? make_optional(store_key_t(jobstate.unindexed_upper_bound->ukey))
        : r_nullopt;

    rfdb::datum_range_iterator data_iter = rfdb::primary_prefix_make_iterator(pkey_prefix,
            js_lower_bound, js_upper_bound.ptr_or_null(), false, reverse);  // snapshot=false, reverse=reverse
    // QQQ: create data_iter fut here, block later.
    icdbf("eicj '%s', lb '%s'\n", debug_str(pkey_prefix).c_str(),
        debug_str(js_lower_bound.str()).c_str());

    // TODO: Apply a workaround for write contention problems mentioned above.
    table_config_t table_config;
    if (!table_config_fut.block_and_deserialize(interruptor, &table_config)) {
        ret.failed.set("missing table config");
        return ret;
    }

    // sindexes_it is casually used to mutate table_config, much later.
    const auto sindexes_it = find_sindex(&table_config.sindexes, index_create_info.sindex_id);

    guarantee(sindexes_it != table_config.sindexes.end());  // TODO: msg, graceful

    const sindex_metaconfig_t &sindex_config = sindexes_it->second;
    guarantee(sindex_config.creation_task_or_nil == info.shared_task_id);  // TODO: msg, graceful

    // TODO: You know, it is kind of sad that we do key/handling fluff redundantly here.

    std::pair<std::vector<std::pair<store_key_t, std::vector<uint8_t>>>, bool> kvs;
    bool more;

    size_t total_bytes_read = 0;

    // We have a loop to ensure we slurp at least one document.
    do {
        icdbf("eicj '%s', lb '%s' loop\n", debug_str(pkey_prefix).c_str(),
            debug_str(js_lower_bound.str()).c_str());
        size_t bytes_read = 0;
        const int target_bytes = recommended_target_bytes(*retry_state);
        kvs = data_iter.query_and_step(txn, interruptor, FDB_STREAMING_MODE_LARGE, target_bytes, &bytes_read);
        total_bytes_read += bytes_read;
        more = kvs.second;
    } while (kvs.first.empty() && more);

    // TODO: Weakly gross that we update parts of retry_state in miscellaneous places in this function.
    retry_state->last_bytes_read = make_optional(total_bytes_read);
    retry_state->last_key_count = make_optional(kvs.first.size());

    icdbf("eicj '%s', first key '%s', reverse=%d exited loop, kvs count = %zu, more = %d\n",
        debug_str(pkey_prefix).c_str(), debug_str(js_lower_bound.str()).c_str(),
        kvs.first.size(), reverse, more);

    // TODO: Maybe FDB should store sindex_disk_info_t, using
    // sindex_reql_version_info_t.

    // TODO: Making this copy is gross -- would be better if compute_keys took sindex_config.
    sindex_disk_info_t index_info = rfdb::sindex_config_to_disk_info(sindex_config.config);

    // Okay, now compute the sindex write.

    // We reuse the same buffer through the loop.
    std::string fdb_key = rfdb::table_index_prefix(index_create_info.table_id,
        index_create_info.sindex_id);
    const size_t index_prefix_size = fdb_key.size();

    for (const auto &elem : kvs.first) {
        icdbf("eicj '%s', lb '%s' loop, elem '%s'\n", debug_str(pkey_prefix).c_str(),
            debug_str(js_lower_bound.str()).c_str(),
            debug_str(elem.first.str()).c_str());
        // TODO: Increase MAX_KEY_SIZE at some point.

        ql::datum_t doc = parse_table_value(as_char(elem.second.data()), elem.second.size());

        try {
            std::vector<store_key_t> keys;
            compute_keys(elem.first, std::move(doc), index_info, &keys, nullptr);

            for (auto &sindex_key : keys) {
                // TODO: Make sure fdb key limits are followed.
                rdbtable_sindex_fdb_key_onto(&fdb_key, sindex_key);
                uint8_t value[1];
                fdb_transaction_set(txn,
                    as_uint8(fdb_key.data()), int(fdb_key.size()),
                    value, 0);
                fdb_key.resize(index_prefix_size);
            }
        } catch (const ql::base_exc_t &) {
            // Do nothing (the row doesn't get put into the index)
        }
    }

    // Maybe the datum range iterator could have methods to compute these values for us.
    // Here is where we add the conflict range.
    {
        std::string lower_bound;
        std::string upper_bound;
        if (!reverse) {
            lower_bound = rfdb::datum_range_lower_bound(pkey_prefix, js_lower_bound);
            // We compute this again below, which is kind of wasteful.
            upper_bound = !more ? rfdb::datum_range_upper_bound(pkey_prefix, js_upper_bound.ptr_or_null()) :
                rfdb::kv_prefix_end(rfdb::index_key_concat(pkey_prefix, kvs.first.back().first));
        } else {
            // Keys are in reverse order, so back() is the smallest key.
            lower_bound = !more ? rfdb::datum_range_lower_bound(pkey_prefix, js_lower_bound) :
                rfdb::datum_range_lower_bound(pkey_prefix, kvs.first.back().first);
            upper_bound = rfdb::datum_range_upper_bound(pkey_prefix, js_upper_bound.ptr_or_null());
        }


        fdb_error_t err = fdb_transaction_add_conflict_range(
                txn,
                as_uint8(lower_bound.data()), int(lower_bound.size()),
                as_uint8(upper_bound.data()), int(upper_bound.size()),
                FDB_CONFLICT_RANGE_TYPE_WRITE);
        guarantee_fdb(err, "Adding conflict range in sindex build failed");  // TODO: fdb, graceful
    }

    if (more) {
        icdbf("eicj '%s', lb '%s', we have more\n",
            debug_str(pkey_prefix).c_str(),
                debug_str(js_lower_bound.str()).c_str());

        guarantee(!kvs.first.empty());

        fdb_index_jobstate new_jobstate;
        if (!reverse) {
            // Increment the pkey lower bound (with kv_prefix_end) since it's inclusive
            // and we need to do that.

            // (This could use std::move instead of std::string, but I don't want to risk
            // bugs.)
            std::string pkey_str = rfdb::kv_prefix_end(std::string(kvs.first.back().first.str()));
            icdbf("eicj '%s', lb '%s' new pkey_str '%s'\n", debug_str(pkey_prefix).c_str(),
                debug_str(js_lower_bound.str()).c_str(), debug_str(pkey_str).c_str());
            new_jobstate = fdb_index_jobstate{
                ukey_string{std::move(pkey_str)},
                std::move(jobstate.unindexed_upper_bound)};
        } else {
            std::string pkey_str = kvs.first.back().first.str();
            new_jobstate = fdb_index_jobstate{
                std::move(jobstate.unindexed_lower_bound),
                make_optional(ukey_string{std::move(pkey_str)})};
        }

        transaction_set_uq_index<index_jobstate_by_task>(txn, info.shared_task_id,
            new_jobstate);

        reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);
        fdb_job_info new_info = update_job_counter(txn, current_clock, info);
        ret.reclaimed.set(std::move(new_info));
    } else {
        transaction_erase_uq_index<index_jobstate_by_task>(txn, info.shared_task_id);

        remove_fdb_job(txn, info);

        // sindexes_it still points into table_config.
        sindexes_it->second.creation_task_or_nil = fdb_shared_task_id{nil_uuid()};
        // Table by name index unchanged.
        // users_by_ids unchanged.
        transaction_set_uq_index<table_config_by_id>(txn, index_create_info.table_id,
            table_config);
        reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
        cv.value++;
        transaction_set_config_version(txn, cv);
        ret.reclaimed = r_nullopt;
    }
    commit(txn, interruptor);
    return ret;
}

// QQQ: Improve index create behavior (make it serial, deal with contention, and such).
