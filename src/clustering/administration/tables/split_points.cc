#include "clustering/administration/tables/split_points.hpp"

#include "clustering/administration/real_reql_cluster_interface.hpp"
#include "math.hpp"   /* for `clamp()` */
#include "rdb_protocol/real_table.hpp"


store_key_t max_store_key() {
    uint8_t buf[MAX_KEY_SIZE];
    for (int i = 0; i < MAX_KEY_SIZE; i++) {
        buf[i] = 255;
    }
    return store_key_t(MAX_KEY_SIZE, buf);
}

static const store_key_t store_key_max = max_store_key();


// TODO: Remove if unused.
/* `interpolate_key()` produces a `store_key_t` that is interpolated between `in1` and
`in2`. For example, if `fraction` is 0.50, the return value will be halfway between `in1`
and `in2`; if it's 0.25, the return value will be closer to `in1`; and so on. This
function is not exact; the only absolute guarantee it provides is that the return value
will lie in the range `in1 <= out <= in2`. */
store_key_t interpolate_key(const store_key_t &in1, const lower_key_bound &in2_param, double fraction) {
    rassert(in2_param.infinite || in1 <= in2_param.key);
    rassert(fraction >= 0 && fraction <= 1);
    const store_key_t &in2 = in2_param.infinite ? in2_param.key : store_key_max;
    const uint8_t *in1_buf = in1.data();
    const uint8_t *in2_buf = in2.data();
    uint8_t out_buf[MAX_KEY_SIZE];

    /* Calculate the shared prefix of `in1` and `in2` */
    size_t i = 0;
    while (i < static_cast<size_t>(in1.size()) &&
           i < static_cast<size_t>(in2.size()) &&
           in1_buf[i] == in2_buf[i]) {
        out_buf[i] = in1_buf[i];
        ++i;
    }

    /* Convert the first non-shared parts of `in1` and `in2` into integers so we can do
    arithmetic on them. If `in1` or `in2` terminates early, pad it with zeroes. This
    isn't perfect but the error doesn't matter for our purposes. */
    uint32_t in1_tail = 0, in2_tail = 0;
    for (size_t j = i; j < i + sizeof(in1_tail); ++j) {
        uint8_t c1 = j < static_cast<size_t>(in1.size()) ? in1_buf[j] : 0;
        uint8_t c2 = j < static_cast<size_t>(in2.size()) ? in2_buf[j] : 0;
        in1_tail = (in1_tail << 8) + c1;
        in2_tail = (in2_tail << 8) + c2;
    }
    rassert(in1_tail <= in2_tail);

    /* Compute an integer representation of the interpolated value */
    uint32_t out_tail = in1_tail * (1 - fraction) + in2_tail * fraction;

    /* Append the interpolated value onto `out_buf`, discarding trailing null bytes */
    size_t num_interp = 0;
    while (num_interp < sizeof(out_tail) && out_tail != 0 && i + num_interp < MAX_KEY_SIZE) {
        out_buf[i + num_interp] = (out_tail >> 24) & 0xFF;
        ++num_interp;
        out_tail <<= 8;
    }

    /* Construct the final result */
    store_key_t out(i + num_interp, out_buf);

    /* For various reasons (rounding errors, corner cases involving keys very close
    together, etc.), it's possible that the above procedure will produce an `out` that is
    not between `in1` and `in2`. Rather than trying to interpolate properly in these
    complicated cases, we just clamp the result. */
    out = clamp(out, in1, in2);

    return out;
}

// The use of decrement_key here is called on keys that are within O(n)
// decrements of the maximum key value.  Thus, the keys are already MAX_KEY_SIZE
// bytes long, and we aren't inflating key lengths.  This is all a hypothetical,
// anyway.
bool decrement_key(store_key_t *key) {
    std::string &str = key->str();
    if (str.empty()) {
        return false;
    } else if (str.back() != 0) {
        str.back() = static_cast<uint8_t>(str.back()) - 1;
        for (int i = str.size(); i < MAX_KEY_SIZE; i++) {
            str.push_back(static_cast<char>(255));
        }
        return true;
    } else {
        str.pop_back();
        return true;
    }
}

// TODO: Remove if unused.
/* `ensure_distinct()` ensures that all of the `store_key_t`s in the given vector are
distinct from eachother. Initially, they should be non-strictly monotonically increasing;
upon return, they will be strictly monotonically increasing. */
void ensure_distinct(std::vector<store_key_t> *split_points) {
    for (size_t i = 1; i < split_points->size(); ++i) {
        /* Make sure the initial condition is met */
        guarantee(split_points->at(i) >= split_points->at(i-1));
    }
    for (size_t i = 1; i < split_points->size(); ++i) {
        /* Normally, we fix any overlaps by just pushing keys forward. */
        while (split_points->at(i) <= split_points->at(i-1)) {
            bool ok = split_points->at(i).increment();
            if (!ok) {
                /* Oops, we ran into the maximum possible key. This is incredibly
                unlikely in practice, but we handle it anyway. */
                size_t j = i;
                while (j > 1 && split_points->at(j) == split_points->at(j-1)) {
                    bool ok2 = decrement_key(&split_points->at(j-1));
                    guarantee(ok2);
                    --j;
                }
            }
        }
    }
}

// TODO: Remove if unused.
void fetch_distribution(
        const namespace_id_t &table_id,
        real_reql_cluster_interface_t *reql_cluster_interface,
        signal_t *interruptor,
        std::map<store_key_t, int64_t> *counts_out)
        THROWS_ONLY(interrupted_exc_t, failed_table_op_exc_t, no_such_table_exc_t) {
    namespace_interface_access_t ns_if_access =
        reql_cluster_interface->get_namespace_repo()->get_namespace_interface(
            table_id, interruptor);
    static const int depth = 2;
    static const int limit = 128;
    distribution_read_t inner_read(depth, limit);
    read_t read(inner_read, profile_bool_t::DONT_PROFILE, read_mode_t::OUTDATED);
    read_response_t resp;
    try {
        ns_if_access.get()->read(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::False, tribool::False, tribool::False)),
            read,
            &resp,
            order_token_t::ignore,
            interruptor);
    } catch (const cannot_perform_query_exc_t &) {
        /* If the table was deleted, this will throw `no_such_table_exc_t` */
        table_basic_config_t dummy;
        reql_cluster_interface->get_table_meta_client()->get_name(table_id, &dummy);
        /* If `get_name()` didn't throw, the table exists but is inaccessible */
        throw failed_table_op_exc_t();
    }
    *counts_out = std::move(
        boost::get<distribution_read_response_t>(resp.response).key_counts);
}

// TODO: Remove if unused.
store_key_t key_for_uuid(uint64_t first_8_bytes) {
    uuid_u uuid;
    memset(uuid.data(), 0, uuid_u::static_size());
    for (size_t i = 0; i < 8; i++) {
        /* Copy one byte at a time to avoid endianness issues */
        uuid.data()[i] = (first_8_bytes >> (8 * (7 - i))) & 0xFF;
    }
    return store_key_t(ql::datum_t(datum_string_t(uuid_to_str(uuid))).print_primary());
}

