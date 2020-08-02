#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

#include <unordered_set>

#include "clustering/admin_op_exc.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/metadata.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "clustering/id_types.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "fdb/jobs/index_create.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/reql_fdb_utils.hpp"
#include "fdb/system_tables.hpp"
#include "rdb_protocol/val.hpp"  // TODO: provisional_table_id in own file

/*
Some ranting about the config cache and config versions...

We suffer a basic problem: If we get a config_version_exc_t, we have to re-evaluate the
query.  At which point do we have to reevaluate the query?  I suppose, the largest
containing expression which contains both the r.table or r.db expression that carried
the out-of-date information, and the code which triggered the exception.

A few things:

1. The expression might have had side effects on the db.  Then we'd be doubling the side
effect on the db.  That's different than an incomplete write.  And it might be a side
effect like tableCreate.  A basic query like

r.expr(["alpha", "beta", "cuck"]).forEach(x => r.db('test').tableCreate(x))

would fail because the db's CV is out-of-date (assuming we don't manually wipe the
config cache after the first tableCreate) -- and the second round would get a conflict
on the table "alpha".

One workaround is to simply always track the provenance of some db/table object or
database id.  Oh, we used the cached name->id mapping as of config version X.  How about
that.

Also possible:  We used the cached table name -> id -> db id mapping as of config version X.

Then, if the config version is out-of-date, we just retry that lookup.  And if that
fails... we're in trouble.

2. It sure is a good thing that we can't put r.db or r.table in a variable.  It isn't a datum.

3. I was going to say that we need to make sure our side effects are in order.  Let's
say we run the query

    r.db('d').table('foo').insert({value: r.http(...)});

Or, God forbid, imagine inserting `{value: r.range(1000).map(x => r.http(...))}`.)

We will want to fail on the db/table lookup _before_ the HTTP query.  Not simply because
it's slow, but because it is very polite not to have an external side effect, period, if
the db or table lookup would have failed.  (I think `insert` unofficially promises the
table argument gets evaluated before the rhs.)

Less dramatically, it's pretty gauche to have

    r.table('dne').insert(r.table('alpha').get(666))

with a lookup failure on `'dne'` happening *after* retrieval from `'alpha'`.  What's
more, we might get a document not found error retrieving from ``alpha'`.

4. I think the solution is a global (per-query) sequence of lookups that got cached and
need to be verified.  And they'd be verified by checking the config version, and if that
fails, by re-performing the lookup.

They'd have to get verified before they actually get *used* in any way.

Once we have to use a value, that means a trip to FDB, at which point we perform _all
prior lookups_ that had been cached.  And then we fill in the data, update the caches,
etc.  (We presumably perform the prior lookups by checking the cv hasn't changed, and
then if it has changed, we re-attempt the lookup.)

5. Here's a problem: When evaluating, we end up throwing exceptions from the wrong
expression.  We might need some work to ensure the "backtrace" is correct.  Fortunately,
general errors aren't handleable by the users, so maybe we can just blithely spit out
the exception.

6. The purpose of the config cache is that we can calculate keys and request documents
from tables without performing a table name -> id lookup.  We _will_ need to handle the
case (in the middle of an fdb transaction) where we operated based on out-of-date data.

7. We also want to plan ahead for when we'll do parallelized evaluation of the same
query, or at least cooperatively concurrent communication to fdb.

*/


/*
So, here's the plan.  Each operation on the db comes with some sequence of "assumed
get-results" (in a particular order) that need to be verified.  If there's
parallelization, these might form a bit of a spaghetti stack.  We can either verify them
by seeing that the reqlfdb_config_version hasn't changed, or by performing the actual
get.

Since we start other FDB operations under the assumption the gets will get a certain result, we may have to retry the FDBTransaction _without_ the assumptions (or with new results for the "assumed get-results" values).

The sequence of assumed get results does not imply causality -- but there might be
causality.

The reqlfdb_config_cache will still wipe the entire cache whenever the config version
updates.  This way we don't have to worry about garbage collecting stale entries to
deleted tables from the cache (some worst case scenario like that).
*/


reqlfdb_config_cache::reqlfdb_config_cache()
    : config_version{0} {}
reqlfdb_config_cache::~reqlfdb_config_cache() {}

void reqlfdb_config_cache::wipe() {
    reqlfdb_config_cache tmp;
    *this = std::move(tmp);
}

void reqlfdb_config_cache::add_db(
        const database_id_t &db_id, const name_string_t &db_name) {
    db_id_index.emplace(db_id, db_name);
    db_name_index.emplace(db_name, db_id);
}

void reqlfdb_config_cache::add_table(
        const namespace_id_t &table_id, counted_t<const rc_wrapper<table_config_t>> config) {
    table_name_index.emplace(
        std::make_pair(config->basic.database, config->basic.name),
        table_id);
    table_id_index.emplace(table_id, std::move(config));
}

std::string unserialize_table_by_name_table_name_part(key_view table_name_part) {
    return std::string(as_char(table_name_part.data), table_name_part.length);
}

std::string unserialize_table_by_name_table_name(key_view key, database_id_t db_id) {
    std::string prefix = unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)});

    key_view chopped = key.guarantee_without_prefix(prefix);
    return unserialize_table_by_name_table_name_part(chopped);
}

ukey_string table_by_name_bound(
        const database_id_t &db_id,
        const std::string &table_name_bound) {
    // Follows the table_by_name_key format.
    return ukey_string{table_by_name_ukey_prefix(db_id) + table_name_bound};
}

std::string table_config_by_name_prefix(const database_id_t &db_id) {
    return unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)});
}

optional<config_info<database_id_t>> reqlfdb_config_cache::try_lookup_cached_db(
        const name_string_t &db_name) const {
    optional<config_info<database_id_t>> ret;
    ASSERT_NO_CORO_WAITING;  // mutex assertion
    auto it = db_name_index.find(db_name);
    if (it != db_name_index.end()) {
        ret.emplace();
        ret->ci_value = it->second;
        ret->ci_cv = config_version;
    }
    return ret;
}

optional<config_info<std::pair<namespace_id_t, counted<const rc_wrapper<table_config_t>>>>>
reqlfdb_config_cache::try_lookup_cached_table(
        const std::pair<database_id_t, name_string_t> &table_name) const {
    optional<config_info<std::pair<namespace_id_t, counted<const rc_wrapper<table_config_t>>>>> ret;
    ASSERT_NO_CORO_WAITING;  // mutex assertion
    auto it = table_name_index.find(table_name);
    if (it != table_name_index.end()) {
        auto jt = table_id_index.find(it->second);
        r_sanity_check(jt != table_id_index.end());

        ret.emplace();
        ret->ci_value = std::make_pair(it->second, jt->second);
        ret->ci_cv = config_version;
    }
    return ret;
}

// NNN: Is this used anymore?
config_info<optional<database_id_t>>
config_cache_retrieve_db_by_name(
        const reqlfdb_config_version config_cache_cv,
        FDBTransaction *txn,
        const name_string_t &db_name, const signal_t *interruptor) {
    fdb_value_fut<database_id_t> fut = transaction_lookup_uq_index<db_config_by_name>(
        txn, db_name);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    // Block here (1st round-trip)
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    if (cv.value < config_cache_cv.value) {
        // Throw a retryable exception.
        // TODO: Should be impossible, just a guarantee failure.
        throw fdb_transaction_exception(REQLFDB_not_committed);
    }

    // Block here (1st round-trip)
    database_id_t id;
    bool present = fut.block_and_deserialize(interruptor, &id);

    config_info<optional<database_id_t>> ret;
    ret.ci_cv = cv;
    if (present) {
        ret.ci_value.set(id);
    }
    return ret;
}

// OOO: Caller of these three fns need to check cv and wipe/refresh config cache.
config_info<optional<std::pair<database_id_t, optional<std::pair<namespace_id_t, table_config_t>>>>>
config_cache_retrieve_db_and_table_by_name(
        FDBTransaction *txn, const name_string_t &db_name,
        const name_string_t &table_name,
        const signal_t *interruptor) {
    using return_type = config_info<optional<std::pair<database_id_t, optional<std::pair<namespace_id_t, table_config_t>>>>>;

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    fdb_value_fut<database_id_t> db_id_fut = transaction_lookup_uq_index<db_config_by_name>(
        txn, db_name);

    return_type ret;
    ret.ci_cv = cv_fut.block_and_deserialize(interruptor);

    std::pair<database_id_t, name_string_t> db_table_name;

    if (!db_id_fut.block_and_deserialize(interruptor, &db_table_name.first)) {
        return ret;
    }
    ret.ci_value.emplace();
    ret.ci_value->first = db_table_name.first;

    db_table_name.second = table_name;

    fdb_value_fut<namespace_id_t> table_id_fut = transaction_lookup_uq_index<table_config_by_name>(
        txn, db_table_name);

    namespace_id_t table_id;
    if (!table_id_fut.block_and_deserialize(interruptor, &table_id)) {
        return ret;
    }

    fdb_value_fut<table_config_t> table_by_id_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);
    table_config_t config;
    bool config_present = table_by_id_fut.block_and_deserialize(interruptor, &config);
    guarantee(config_present);  // TODO: Nice error?  FDB in bad state.
    guarantee(db_table_name.first == config.basic.database);  // TODO: Nice error?  FDB in bad state.
    guarantee(db_table_name.second == config.basic.name);  // TODO: Nice error?  FDB in bad state.

    ret.ci_value->second.emplace(table_id, std::move(config));

    return ret;
}

database_id_t
expect_retrieve_db(
        FDBTransaction *txn, const provisional_db_id &prov_db,
        const signal_t *interruptor) {
    fdb_value_fut<database_id_t> fut = transaction_lookup_uq_index<db_config_by_name>(txn, prov_db.db_name);
    database_id_t ret;
    if (!fut.block_and_deserialize(interruptor, &ret)) {
        rfail_src(prov_db.bt, ql::base_exc_t::OP_FAILED,
            "Database `%s` does not exist.", prov_db.db_name.c_str());
    }
    return ret;
}

config_info<std::pair<namespace_id_t, table_config_t>>
expect_retrieve_table(
        FDBTransaction *txn, const provisional_table_id &prov_table,
        const signal_t *interruptor) {
    config_info<optional<std::pair<database_id_t, optional<std::pair<namespace_id_t, table_config_t>>>>> result
        = config_cache_retrieve_db_and_table_by_name(txn,
            prov_table.prov_db.db_name, prov_table.table_name,
            interruptor);

    // TODO: Duplicates failing logic in provisional_to_table.
    if (!result.ci_value.has_value()) {
        rfail_db_not_found(prov_table.bt, prov_table.prov_db.db_name);
    }

    if (!result.ci_value->second.has_value()) {
        rfail_table_dne_src(prov_table.bt, prov_table.prov_db.db_name,
            prov_table.table_name);
    }

    config_info<std::pair<namespace_id_t, table_config_t>> ret;
    ret.ci_cv = result.ci_cv;
    ret.ci_value = std::move(*result.ci_value->second);
    return ret;
}


config_info<optional<auth::user_t>>
config_cache_retrieve_user_by_name(
        const reqlfdb_config_version config_cache_cv, FDBTransaction *txn,
        const auth::username_t &username, const signal_t *interruptor) {
    fdb_value_fut<auth::user_t> user_fut = transaction_get_user(txn, username);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    if (cv.value < config_cache_cv.value) {
        // Throw a retryable exception.
        throw fdb_transaction_exception(REQLFDB_not_committed);
    }

    config_info<optional<auth::user_t>> ret;
    ret.ci_cv = cv;

    auth::user_t user;
    if (user_fut.block_and_deserialize(interruptor, &user)) {
        ret.ci_value.set(std::move(user));
    }
    return ret;
}

bool config_cache_db_create(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const name_string_t &db_name,
        const database_id_t &new_db_id,
        const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.
    guarantee(db_name != artificial_reql_cluster_interface_t::database_name,
        "config_cache_db_create should never get queries for system tables");

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    auth::fdb_user_fut<auth::config_permission> auth_fut
        = user_context.transaction_require_config_permission(txn);
    fdb_value_fut<database_id_t> fut = transaction_lookup_uq_index<db_config_by_name>(
        txn, db_name);

    auth_fut.block_and_check(interruptor);

    fdb_value value = future_block_on_value(fut.fut, interruptor);
    if (value.present) {
        // A db with this name already exists.
        return false;
    }
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    ASSERT_NO_CORO_WAITING;

    transaction_set_uq_index<db_config_by_id>(txn, new_db_id, db_name);
    transaction_set_uq_index<db_config_by_name>(txn, db_name, new_db_id);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return true;
}

// TODO: Note that when we drop a db, we pretend its tables don't exist.  But the user still retains references to the table until the cleanup job has removed the table.  Do system tables expose this information inconsistently at all?

// TODO: Be sure to test that we can actually remove tables as non-admin user with specific config permissions on that... whatever.  That is, make sure we don't do 

void help_remove_user_uuid_references(
        FDBTransaction *txn,
        const uuid_u &db_or_table_id,
        const signal_t *interruptor) {
    std::string prefix = plain_index_skey_prefix<users_by_ids>(db_or_table_id);
    std::vector<std::pair<auth::username_t, fdb_value_fut<auth::user_t>>> user_futs;
    transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
            [&](const FDBKeyValue &kv) {
        key_view whole_key{void_as_uint8(kv.key), kv.key_length};
        key_view key = whole_key.guarantee_without_prefix(prefix);

        auth::username_t username = users_by_username::parse_ukey(key);
        user_futs.push_back(std::make_pair(username,
            transaction_lookup_uq_index<users_by_username>(txn, username)));
        return true;
    });

    for (auto &pair : user_futs) {
        auth::user_t old_user = pair.second.block_and_deserialize(interruptor);
        auth::user_t new_user = old_user;
        new_user.set_db_or_table_permissions_indeterminate(db_or_table_id);
        transaction_modify_user(txn, pair.first, old_user, new_user);
    }
}

// TODO: Use uniform reql datum primary key serialization, how about that idea?

// db_name must come from the db (in the same txn).
void config_cache_db_drop_uuid(
        FDBTransaction *txn, const auth::user_context_t &user_context,
        const database_id_t &db_id, const name_string_t &db_name,
        const signal_t *interruptor) {
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    // TODO: We could get the table id's concurrently (in a coro).
    std::vector<namespace_id_t> table_ids;
    {
        std::string prefix = table_config_by_name_prefix(db_id);
        transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
        [&](const FDBKeyValue &kv) {
            namespace_id_t table_id;
            deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &table_id);
            table_ids.push_back(table_id);
            return true;
        });
    }

    // TODO: We could totally get the user fut concurrently above, pass it in here.
    auth::fdb_user_fut<auth::db_multi_table_config_permission> auth_fut
        = user_context.transaction_require_db_multi_table_config_permission(txn,
            db_id,
            std::move(table_ids));

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    // Add the db_drop job, and remove the db.

    // TODO: This node should claim the job (and add logic to immediately execute it).
    fdb_node_id claiming_node_id{nil_uuid()};
    fdb_shared_task_id task_id{generate_uuid()};
    fdb_job_description desc{
        fdb_job_type::db_drop_job,
        fdb_job_db_drop::make(db_id)};

    // TODO: We could split up the read/write portion of add_fdb_job, mix with above,
    // and avoid double round-trip latency.

    // We _could_ pass in self_node_id, return the fdb_job_info, and claim the job at
    // creation.  Right now, we don't.
    fdb_job_info ignored = add_fdb_job(txn, task_id, claiming_node_id, std::move(desc), interruptor);
    (void)ignored;

    // Check the auth fut after a round-trip in add_fdb_job.
    auth_fut.block_and_check(interruptor);

    // I'll note we call help_remove_user_uuid_references *after* checking the auth_fut,
    // so we don't somehow overwrite the user_t in such a way that removes our
    // permissions to drop the db or its tables.

    // TODO: Parallize this relative to add_fdb_job, maybe the auth fut.
    help_remove_user_uuid_references(txn, db_id.value, interruptor);

    transaction_erase_uq_index<db_config_by_id>(txn, db_id);
    transaction_erase_uq_index<db_config_by_name>(txn, db_name);

    cv.value++;
    transaction_set_config_version(txn, cv);
}

optional<database_id_t> config_cache_db_drop(
        FDBTransaction *txn, const auth::user_context_t &user_context,
        const name_string_t &db_name, const signal_t *interruptor) {
    fdb_value_fut<database_id_t> fut = transaction_lookup_uq_index<db_config_by_name>(
        txn, db_name);

    fdb_value value = future_block_on_value(fut.fut, interruptor);
    database_id_t db_id;
    if (!deserialize_off_fdb_value(value, &db_id)) {
        return r_nullopt;
    }

    config_cache_db_drop_uuid(txn, user_context, db_id, db_name, interruptor);
    return make_optional(db_id);
}

// Returns TABLE_CONFIG_BY_NAME range in database db_id, in [lower_bound_table_name,
// +infinity), if closed, and (lower_bound_table_name, +infinity), if open.
fdb_future transaction_get_table_range(
        FDBTransaction *txn, const database_id_t db_id,
        const std::string &lower_bound_table_name, bool closed,
        FDBStreamingMode streaming_mode) {

    std::string lower = unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        table_by_name_bound(db_id, lower_bound_table_name));
    std::string upper = prefix_end(unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)}));

    return fdb_future{fdb_transaction_get_range(txn,
        as_uint8(lower.data()), int(lower.size()), !closed, 1,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper.data()), int(upper.size())),
        0,
        0,
        streaming_mode,
        0,
        false,
        false)};
}

void help_remove_table(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const table_config_t &config,
        const signal_t *interruptor) {
    // Wipe table config (from pkey and indices), and wipe table contents.
    transaction_erase_uq_index<table_config_by_id>(txn, table_id);
    transaction_erase_uq_index<table_config_by_name>(txn,
        std::make_pair(config.basic.database, config.basic.name));

    // TODO: Parallelize this.
    // For any sindexes with jobs, remove their jobs.
    for (auto &&pair : config.sindexes) {
        fdb_shared_task_id task = pair.second.creation_task_or_nil;
        if (!task.value.is_nil()) {
            remove_fdb_task_and_jobs(txn, task, interruptor);
        }
    }

    // TODO: And also, parallelize this with it.
    help_remove_user_uuid_references(txn, table_id.value, interruptor);

    // Delete table data.
    std::string prefix = rfdb::table_key_prefix(table_id);
    transaction_clear_prefix_range(txn, prefix);
}

bool help_remove_table_if_exists(
        FDBTransaction *txn,
        database_id_t db_id,
        const std::string &table_name,
        const signal_t *interruptor) {
    // TODO: Split this function up into future creation part and blocking part, to
    // avoid multiple latency round-trips.

    ukey_string table_index_key = table_by_unverified_name_key(db_id, table_name);
    fdb_value_fut<namespace_id_t> table_by_name_fut{transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key)};

    namespace_id_t table_id;
    bool table_present = table_by_name_fut.block_and_deserialize(interruptor, &table_id);
    if (!table_present) {
        return false;
    }

    fdb_value_fut<table_config_t> table_by_id_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);
    table_config_t config = table_by_id_fut.block_and_deserialize(interruptor);

    help_remove_table(txn, table_id, config, interruptor);
    return true;
}


optional<std::pair<namespace_id_t, table_config_t>> config_cache_table_drop(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const provisional_db_id &prov_db,
        const name_string_t &table_name,
        const signal_t *interruptor) {

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    database_id_t db_id = expect_retrieve_db(txn, prov_db, interruptor);

    fdb_value_fut<namespace_id_t> table_by_name_fut = transaction_lookup_uq_index<table_config_by_name>(
        txn, std::make_pair(db_id, table_name));
    fdb_future db_by_id_fut = transaction_lookup_uq_index<db_config_by_id>(
        txn, db_id);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    {
        fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);
        guarantee(db_by_id_value.present, "db missing, fdb state invalid");  // TODO: Fdb, error message, etc.
    }

    namespace_id_t table_id;
    if (!table_by_name_fut.block_and_deserialize(interruptor, &table_id)) {
        return r_nullopt;
    }

    // We use the table_id to read the table config to produce pretty output for the user.
    // We use the table_id to check permissions, too.
    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(txn, db_id, table_id);

    fdb_value_fut<table_config_t> table_by_id_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    auth_fut.block_and_check(interruptor);
    fdb_value table_by_id_value = future_block_on_value(table_by_id_fut.fut, interruptor);
    table_config_t config;
    if (!deserialize_off_fdb_value(table_by_id_value, &config)) {
        // TODO: graceful error handling for corrupt fdb
        crash("No table_config_by_id for key found in index");
    }

    // Okay, the db's present, and the table's present.  Drop the table.

    help_remove_table(txn, table_id, config, interruptor);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return make_optional(std::make_pair(table_id, std::move(config)));
}

bool config_cache_table_create(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const namespace_id_t &new_table_id,
        const table_config_t &config,
        const signal_t *interruptor) {
    const database_id_t db_id = config.basic.database;
    const name_string_t &table_name = config.basic.name;

    std::pair<database_id_t, name_string_t> table_index_key{db_id, table_name};

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    auth::fdb_user_fut<auth::db_config_permission> auth_fut
        = user_context.transaction_require_db_config_permission(txn, db_id);
    fdb_value_fut<namespace_id_t> table_by_name_fut = transaction_lookup_uq_index<table_config_by_name>(
        txn, table_index_key);
    fdb_future db_by_id_fut
        = transaction_lookup_uq_index<db_config_by_id>(txn, db_id);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    auth_fut.block_and_check(interruptor);
    fdb_value table_by_name_value
        = future_block_on_value(table_by_name_fut.fut, interruptor);

    if (table_by_name_value.present) {
        // Table already exists.
        return false;
    }

    {
        fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);
        guarantee(db_by_id_value.present, "Db by id missing, invalid fdb state");  // TODO: fdb, msg, etc.
    }

    ASSERT_NO_CORO_WAITING;

    // Okay, the db's present, the table is not present.  Create the table.

    transaction_set_uq_index<table_config_by_id>(txn, new_table_id, config);
    transaction_set_uq_index<table_config_by_name>(txn, table_index_key, new_table_id);

    cv.value++;
    transaction_set_config_version(txn, cv);

    return true;
}

std::vector<name_string_t> config_cache_db_list_sorted(
        FDBTransaction *txn,
        const signal_t *interruptor) {
    std::string db_by_name_prefix = db_config_by_name::prefix;
    std::vector<name_string_t> db_names;
    transaction_read_whole_range_coro(txn,
        db_by_name_prefix, prefix_end(db_by_name_prefix), interruptor,
        [&db_names, &db_by_name_prefix](const FDBKeyValue &kv) {
            key_view whole_key{void_as_uint8(kv.key), kv.key_length};
            key_view key = whole_key.guarantee_without_prefix(db_by_name_prefix);
            name_string_t name = db_config_by_name::parse_ukey(key);
            // We deserialize the value (but don't use it) just as a sanity test.
            db_config_by_name::value_type db_id;
            deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &db_id);
            db_names.push_back(std::move(name));
            return true;
        });
    // db_names is in sorted order.
    return db_names;
}

std::vector<std::pair<database_id_t, name_string_t>> config_cache_db_list_sorted_by_id(
        FDBTransaction *txn,
        const signal_t *interruptor) {
    std::string db_by_id_prefix = db_config_by_id::prefix;
    std::vector<std::pair<database_id_t, name_string_t>> ret;
    transaction_read_whole_range_coro(txn,
        db_by_id_prefix, prefix_end(db_by_id_prefix), interruptor,
            [&ret, &db_by_id_prefix](const FDBKeyValue &kv) {
        key_view whole_key{void_as_uint8(kv.key), kv.key_length};
        key_view key = whole_key.guarantee_without_prefix(db_by_id_prefix);
        database_id_t db_id = db_config_by_id::parse_ukey(key);
        name_string_t db_name;
        deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &db_name);
        ret.emplace_back(db_id, db_name);
        return true;
    });
    return ret;
}

// TODO: If we can't iterate the tables in a single txn, we could do a snapshot read
// or check the config version, or something.

// This is listed in ascending order.
std::vector<name_string_t> config_cache_table_list_sorted(
        FDBTransaction *txn,
        provisional_db_id prov_db,
        const signal_t *interruptor) {

    database_id_t db_id = expect_retrieve_db(txn, prov_db, interruptor);

    std::string prefix = table_config_by_name_prefix(db_id);
    std::vector<name_string_t> table_names;
    transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
    [&prefix, &table_names](const FDBKeyValue &kv) {
        key_view whole_key{void_as_uint8(kv.key), kv.key_length};
        key_view table_name_part = whole_key.guarantee_without_prefix(prefix);
        // Basically unserialize_table_by_name_table_name without recomputing
        // the prefix.
        std::string table_name
            = unserialize_table_by_name_table_name_part(table_name_part);
        name_string_t name;
        bool res = name.assign_value(table_name);
        guarantee(res, "invalid table name unserialized from table_by_name key");
        table_names.push_back(std::move(name));
        return true;
    });

    return table_names;
}

MUST_USE optional<std::pair<reqlfdb_config_version, optional<fdb_job_info>>> config_cache_sindex_create(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const provisional_table_id &table,
        const std::string &index_name,
        const sindex_id_t &new_sindex_id,
        const fdb_shared_task_id &new_index_create_task_id,
        const sindex_config_t &sindex_config,
        const signal_t *interruptor,
        const ql::backtrace_id_t bt) {
    config_info<std::pair<namespace_id_t, table_config_t>> info
        = expect_retrieve_table(txn, table, interruptor);
    const namespace_id_t &table_id = info.ci_value.first;
    table_config_t &table_config = info.ci_value.second;
    const database_id_t &db_id = table_config.basic.database;
    reqlfdb_config_version cv = info.ci_cv;

    rcheck_src(bt, index_name != table_config.basic.primary_key,
        ql::base_exc_t::LOGIC,
        strprintf("Index name conflict: `%s` is the name of the primary key.",
            index_name.c_str()));

    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, db_id, table_id);

    const std::string pkey_prefix = rfdb::table_pkey_prefix(table_id);
    const std::string pkey_prefix_end = prefix_end(pkey_prefix);
    fdb_future last_key_fut{fdb_transaction_get_key(txn,
        FDB_KEYSEL_LAST_LESS_THAN(
            as_uint8(pkey_prefix_end.data()),
            int(pkey_prefix_end.size())),
        false)};

    auth_fut.block_and_check(interruptor);

    // Two common situations:  (1) the table is empty, (2) the table is not empty.
    const key_view last_key_view = future_block_on_key(last_key_fut.fut, interruptor);
    const bool table_has_data = last_key_view.has_prefix(pkey_prefix);

    const fdb_shared_task_id task_id_or_nil
        = table_has_data ? new_index_create_task_id : fdb_shared_task_id{nil_uuid()};

    if (!table_config.sindexes.emplace(index_name,
            sindex_metaconfig_t{sindex_config, new_sindex_id, task_id_or_nil}).second) {
        return r_nullopt;
    }

    optional<fdb_job_info> job_info_ret;
    if (table_has_data) {
        // TODO: This node should claim the job.
        fdb_node_id claiming_node_id{nil_uuid()};

        fdb_job_description desc{
            fdb_job_type::index_create_job,
            fdb_job_index_create{table_id, new_sindex_id},
        };

        // TODO: We could split up the read/write portion of add_fdb_job, mix with above,
        // and avoid double round-trip latency.

        job_info_ret.set(add_fdb_job(txn, new_index_create_task_id, claiming_node_id,
            std::move(desc), interruptor));

        key_view pkey_only = last_key_view.without_prefix(int(pkey_prefix.size()));
        std::string upper_bound_str(as_char(pkey_only.data), size_t(pkey_only.length));
        upper_bound_str.push_back('\0');
        fdb_index_jobstate jobstate{ukey_string{""}, ukey_string{upper_bound_str}};
        transaction_set_uq_index<index_jobstate_by_task>(txn, new_index_create_task_id, jobstate);
    }

    // Table by name index unchanged.
    transaction_set_uq_index<table_config_by_id>(txn, table_id, table_config);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return make_optional(std::make_pair(cv, std::move(job_info_ret)));
}


void help_erase_sindex_content(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const sindex_metaconfig_t &cfg,
        const signal_t *interruptor) {
    if (!cfg.creation_task_or_nil.value.is_nil()) {
        remove_fdb_task_and_jobs(txn, cfg.creation_task_or_nil, interruptor);
        transaction_erase_uq_index<index_jobstate_by_task>(txn, cfg.creation_task_or_nil);
    }

    transaction_clear_prefix_range(txn, rfdb::table_index_prefix(table_id, cfg.sindex_id));
}

// TODO: Users' db/table config permissions ought to get cleaned up when we drop a db or table.

optional<reqlfdb_config_version> config_cache_sindex_drop(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const provisional_table_id &table,
        const std::string &index_name,
        const signal_t *interruptor) {
    config_info<std::pair<namespace_id_t, table_config_t>> info
        = expect_retrieve_table(txn, table, interruptor);
    // TODO: If we don't have db read permissions, are we allowed to discover whether the table exists or not in the db?
    const namespace_id_t &table_id = info.ci_value.first;
    table_config_t &table_config = info.ci_value.second;
    const database_id_t &db_id = table_config.basic.database;
    reqlfdb_config_version cv = info.ci_cv;

    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, db_id, table_id);

    auth_fut.block_and_check(interruptor);

    auto sindexes_it = table_config.sindexes.find(index_name);
    if (sindexes_it == table_config.sindexes.end()) {
        // Index simply doesn't exist.
        return r_nullopt;
    }

    help_erase_sindex_content(txn, table_id, sindexes_it->second, interruptor);

    table_config.sindexes.erase(sindexes_it);

    // Table by name index unchanged.
    transaction_set_uq_index<table_config_by_id>(txn, table_id, table_config);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return make_optional(cv);
}

optional<table_config_t> config_cache_get_table_config_without_cv_check(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const signal_t *interruptor) {
    fdb_value_fut<table_config_t> table_config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    optional<table_config_t> ret;
    ret.emplace();
    if (!table_config_fut.block_and_deserialize(interruptor, &ret.get())) {
        ret.reset();
    }
    return ret;
}

config_info<rename_result> config_cache_sindex_rename(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const provisional_table_id &table,
        const std::string &old_name,
        const std::string &new_name,
        bool overwrite,
        const signal_t *interruptor,
        const ql::backtrace_id_t bt) {
    config_info<std::pair<namespace_id_t, table_config_t>> info
        = expect_retrieve_table(txn, table, interruptor);
    // TODO: If we don't have db read permissions, are we allowed to discover whether the table exists or not in the db?
    const namespace_id_t &table_id = info.ci_value.first;
    table_config_t &table_config = info.ci_value.second;
    const database_id_t &db_id = table_config.basic.database;
    reqlfdb_config_version cv = info.ci_cv;

    rcheck_src(bt, old_name != table_config.basic.primary_key,
           ql::base_exc_t::LOGIC,
           strprintf("Index name conflict: `%s` is the name of the primary key.",
                     old_name.c_str()));
    rcheck_src(bt, new_name != table_config.basic.primary_key,
           ql::base_exc_t::LOGIC,
           strprintf("Index name conflict: `%s` is the name of the primary key.",
                     new_name.c_str()));

    // TODO: Copy/pasted config_cache_sindex_drop.
    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, db_id, table_id);

    auth_fut.block_and_check(interruptor);

    auto sindexes_it = table_config.sindexes.find(old_name);
    if (sindexes_it == table_config.sindexes.end()) {
        // Index simply doesn't exist.
        return config_info<rename_result>{rename_result::old_not_found, cv};
    }

    if (old_name == new_name) {
        // Avoids sindex drop/overwrite logic below, but we did confirm the index
        // actually exists.
        return config_info<rename_result>{rename_result::success, cv};
    }

    sindex_metaconfig_t fdb_sindex_config = std::move(sindexes_it->second);
    table_config.sindexes.erase(sindexes_it);

    if (overwrite) {
        auto new_name_it = table_config.sindexes.find(new_name);
        if (new_name_it != table_config.sindexes.end()) {
            help_erase_sindex_content(txn, table_id, new_name_it->second, interruptor);
            table_config.sindexes.erase(new_name_it);
        }
    }

    bool inserted = table_config.sindexes.emplace(new_name, std::move(fdb_sindex_config)).second;
    if (!inserted) {
        return config_info<rename_result>{rename_result::new_already_exists, cv};
    }

    // Table by name index unchanged.
    transaction_set_uq_index<table_config_by_id>(txn, table_id, table_config);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return config_info<rename_result>{rename_result::success, cv};
}

// Returns if the write hook config previously existed.
std::pair<bool, reqlfdb_config_version> config_cache_set_write_hook(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const provisional_table_id &prov_table,
        const optional<write_hook_config_t> &new_write_hook_config,
        const signal_t *interruptor) {
    config_info<std::pair<namespace_id_t, table_config_t>>
        info = expect_retrieve_table(txn, prov_table, interruptor);

    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, info.ci_value.second.basic.database, info.ci_value.first);

    auth_fut.block_and_check(interruptor);

    table_config_t &cfg = info.ci_value.second;

    bool old_existed = cfg.write_hook.has_value();

    cfg.write_hook = new_write_hook_config;
    transaction_set_uq_index<table_config_by_id>(txn, info.ci_value.first, cfg);

    reqlfdb_config_version cv = info.ci_cv;
    cv.value++;
    transaction_set_config_version(txn, cv);

    return std::make_pair(old_existed, cv);
}

fdb_value_fut<auth::user_t> transaction_get_user(
        FDBTransaction *txn,
        const auth::username_t &username) {
    return transaction_lookup_uq_index<users_by_username>(txn, username);
}

void transaction_set_user(
        FDBTransaction *txn,
        const auth::username_t &username,
        const auth::user_t &user) {
    transaction_set_uq_index<users_by_username>(txn, username, user);
}

void transaction_create_user(
        FDBTransaction *txn,
        const auth::username_t &username,
        const auth::user_t &user) {
    transaction_set_uq_index<users_by_username>(txn, username, user);

    std::unordered_set<uuid_u> uuids = get_index_uuids(user);
    for (const uuid_u &uuid : uuids) {
        transaction_set_plain_index<users_by_ids>(txn, uuid, username, "");
    }
}

void transaction_modify_user(
        FDBTransaction *txn,
        const auth::username_t &username,
        const auth::user_t &old_user,
        const auth::user_t &new_user) {
    transaction_set_uq_index<users_by_username>(txn, username, new_user);

    std::unordered_set<uuid_u> old_uuids = get_index_uuids(old_user);
    std::unordered_set<uuid_u> new_uuids = get_index_uuids(new_user);

    for (const uuid_u &uuid : old_uuids) {
        auto it = new_uuids.find(uuid);
        if (it != new_uuids.end()) {
            new_uuids.erase(it);
        } else {
            // It's in old but not in new -- erase.
            transaction_erase_plain_index<users_by_ids>(txn, uuid, username);
        }
    }

    for (const uuid_u &uuid : new_uuids) {
        transaction_set_plain_index<users_by_ids>(txn, uuid, username, "");
    }
}


void transaction_erase_user(
        FDBTransaction *txn,
        const auth::username_t &username,
        const auth::user_t &old_user_value) {
    guarantee(!username.is_admin(), "transaction_erase_user on admin user");
    transaction_erase_uq_index<users_by_username>(txn, username);

    std::unordered_set<uuid_u> uuids = get_index_uuids(old_user_value);

    for (const uuid_u &uuid : uuids) {
        transaction_erase_plain_index<users_by_ids>(txn, uuid, username);
    }
}
