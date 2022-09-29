#include "fdb/jobs/index_create.hpp"

#include "clustering/tables/table_metadata.hpp"
#include "debug.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs/job_utils.hpp"
#include "fdb/retry_loop.hpp"  // TODO: Remove include when it becomes unused
#include "fdb/system_tables.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/btree.hpp"  // For compute_keys
#include "rdb_protocol/serialize_datum.hpp"

// #define icdbf(...) debugf(__VA_ARGS__)
#define icdbf(...)

/* Please note that we build secondary indexes in reverse order now, but this discussion
talks as if we build it in forward order.

We encounter a question of conflict resolution when building a secondary index.  The
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


struct index_create_retry_state {
    uint64_t retry_count = 0;
    optional<bool> jobstate_claim_mutable;
    optional<size_t> last_bytes_read;
    optional<size_t> last_key_count;
    optional<store_key_t> last_key_spanned;
};

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

    // Lower bound is the min key, "".
    const store_key_t js_lower_bound("");
    optional<store_key_t> js_upper_bound
        = jobstate.unindexed_upper_bound.has_value()
        ? make_optional(store_key_t(jobstate.unindexed_upper_bound->ukey))
        : r_nullopt;

    rfdb::datum_range_iterator data_iter = rfdb::primary_prefix_make_iterator(pkey_prefix,
        js_lower_bound, js_upper_bound.ptr_or_null(), false, true);  // snapshot=false, reverse=true
    // QQQ: create data_iter fut here, block later.
    icdbf("eicj '%s', ub '%s'\n", debug_str(pkey_prefix).c_str(),
        js_upper_bound.has_value() ? debug_str(js_upper_bound->str()).c_str() : "(+infinity)");

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
        icdbf("eicj '%s', ub '%s' loop\n", debug_str(pkey_prefix).c_str(),
            js_upper_bound.has_value() ? debug_str(js_upper_bound->str()).c_str() : "(+infinity)");

        size_t bytes_read = 0;
        const int target_bytes = recommended_target_bytes(*retry_state);
        kvs = data_iter.query_and_step(txn, interruptor, FDB_STREAMING_MODE_LARGE, target_bytes, &bytes_read);
        total_bytes_read += bytes_read;
        more = kvs.second;
    } while (kvs.first.empty() && more);

    // TODO: Weakly gross that we update parts of retry_state in miscellaneous places in this function.
    retry_state->last_bytes_read = make_optional(total_bytes_read);
    retry_state->last_key_count = make_optional(kvs.first.size());
    retry_state->last_key_spanned = make_optional(kvs.first.empty() ? store_key_t() : kvs.first.back().first);

    icdbf("eicj '%s', first key '%s', exited loop, kvs count = %zu, more = %d\n",
        debug_str(pkey_prefix).c_str(),
        js_upper_bound.has_value() ? debug_str(js_upper_bound->str()).c_str() : "(+infinity)",
        kvs.first.size(), more);

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
        icdbf("eicj '%s', ub '%s' loop, elem '%s'\n", debug_str(pkey_prefix).c_str(),
            js_upper_bound.has_value() ? debug_str(js_upper_bound->str()).c_str() : "(+infinity)",
            debug_str(elem.first.str()).c_str());
        // TODO: Increase MAX_KEY_SIZE at some point.

        ql::datum_t doc = ql::parse_table_value(as_char(elem.second.data()), elem.second.size());

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

    // Here is where we add the conflict range.
    {
        // Keys are in reverse order, so back() is the smallest key.  It looks silly to
        // use datum_range_lower_bound with js_lower_bound instead of pkey_prefix, but
        // that's precisely what the datum_range_iterator computes, so we might as well
        // reproduce the exact interval (and be less fragile w.r.t. future indexing code
        // changes).
        std::string lower_bound = !more ?
            rfdb::datum_range_lower_bound(pkey_prefix, js_lower_bound) :
            rfdb::datum_range_lower_bound(pkey_prefix, kvs.first.back().first);
        std::string upper_bound = rfdb::datum_range_upper_bound(pkey_prefix, js_upper_bound.ptr_or_null());


        fdb_error_t err = fdb_transaction_add_conflict_range(
                txn,
                as_uint8(lower_bound.data()), int(lower_bound.size()),
                as_uint8(upper_bound.data()), int(upper_bound.size()),
                FDB_CONFLICT_RANGE_TYPE_WRITE);
        if (err != 0) {
            throw fdb_transaction_exception(err);
        }
    }

    if (more) {
        icdbf("eicj '%s', ub '%s', we have more\n",
            debug_str(pkey_prefix).c_str(),
            js_upper_bound.has_value() ? debug_str(js_upper_bound->str()).c_str() : "(+infinity)");

        guarantee(!kvs.first.empty());

        const std::string &pkey_str = kvs.first.back().first.str();
        fdb_index_jobstate new_jobstate = fdb_index_jobstate{
            jobstate.claimed_bound.has_value() && jobstate.claimed_bound->ukey < pkey_str
            ? jobstate.claimed_bound
            : r_nullopt,
            make_optional(ukey_string{pkey_str})};

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

MUST_USE fdb_error_t execute_index_create_job(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        const fdb_job_info &info_param,
        const fdb_job_index_create &index_create_info,
        job_execution_result *result_out) {
    index_create_retry_state retry_state;
    bool made_claim = false;
    fdb_job_info new_info;
    const fdb_job_info *info_to_pass = &info_param;
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
    [&](FDBTransaction *txn) {
        if (!made_claim && retry_state.retry_count >= 3 && retry_state.last_key_spanned.has_value()) {
            fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
            fdb_value_fut<fdb_job_info> real_info_fut
                = transaction_get_real_job_info(txn, *info_to_pass);
            fdb_value_fut<fdb_index_jobstate> jobstate_fut
                = transaction_lookup_uq_index<index_jobstate_by_task>(txn, info_to_pass->shared_task_id);

            if (!block_and_check_info(*info_to_pass, std::move(real_info_fut), interruptor)) {
                *result_out = job_execution_result{};
                return;
            }

            fdb_index_jobstate jobstate;
            if (!jobstate_fut.block_and_deserialize(interruptor, &jobstate)) {
                result_out->failed.set("jobstate in invalid state");
                return;
            }

            if (!jobstate.claimed_bound.has_value() ||
                retry_state.last_key_spanned->str() < jobstate.claimed_bound->ukey) {

                // Compute a new jobstate and write it.
                fdb_index_jobstate new_jobstate = fdb_index_jobstate{
                    make_optional(ukey_string{retry_state.last_key_spanned->str()}),
                    jobstate.unindexed_upper_bound};
                transaction_set_uq_index<index_jobstate_by_task>(txn, info_to_pass->shared_task_id,
                    new_jobstate);
            }

            reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);
            new_info = update_job_counter(txn, current_clock, *info_to_pass);

            // Now commit the txn... and reset it so that we roll into
            // execute_index_create_job again.
            commit(txn, interruptor);
            fdb_transaction_reset(txn);
            // Success.
            made_claim = true;
            info_to_pass = &new_info;
        }


        *result_out = execute_index_create_job(interruptor, txn, *info_to_pass,
            index_create_info,
            &retry_state);
    });
    return loop_err;
}

// QQQ: Improve index create behavior (make it serial, deal with contention, and such).
