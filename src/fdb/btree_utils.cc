#include "fdb/btree_utils.hpp"

#include "debug.hpp"
#include "math.hpp"
#include "rdb_protocol/btree.hpp"

// TODO: Remove.
#define btubugf(...)
// #define btubugf(...) debugf(__VA_ARGS__)

namespace rfdb {

static const size_t LARGE_VALUE_SPLIT_SIZE = 16384;
static const char LARGE_VALUE_FIRST_PREFIX = '\x30';
static const char LARGE_VALUE_SECOND_PREFIX = '\x31';
static const char LARGE_VALUE_SUFFIX_LENGTH = 5;  // prefix char followed by 4-digit hex value

// TODO: Try making callers avoid string copy (by not taking a const string& themselves).
std::string kv_prefix(const std::string &kv_location) {
    std::string ret = kv_location;
    ret.push_back('\0');
    return ret;
}

std::string kv_prefix(std::string &&kv_location) {
    std::string ret = std::move(kv_location);
    ret.push_back('\0');
    return ret;
}

std::string kv_prefix_end(const std::string &kv_location) {
    std::string ret = kv_location;
    ret.push_back(1);
    return ret;
}

std::string kv_prefix_end(std::string &&kv_location) {
    std::string ret = std::move(kv_location);
    ret.push_back(1);
    return ret;
}


void append_bigendian_hex16(std::string *str, uint16_t val) {
    push_hex(str, val >> 8);
    push_hex(str, val);
}

uint8_t hexvalue(uint8_t x) {
    if (isdigit(x)) {
        return x - '0';
    } else if (x >= 'a' && x <= 'f') {
        static_assert('f' - 'a' == 5, "non-exotic charset");  // Autism-grade standards compliance
        return 10 + (x - 'a');
    } else if (x >= 'A' && x <= 'F') {
        // TODO: Rename push_hex to push_lowercase_hex or something, avoid this
        // capitalized check.
        static_assert('F' - 'A' == 5, "non-exotic charset");
        return 10 + (x - 'A');
    } else {
        crash("Invalid hex digit");
    }
}

uint16_t decode_bigendian_hex16(const uint8_t *data, size_t length) {
    guarantee(length == 4);
    uint16_t builder = 0;
    for (size_t i = 0; i < 4; ++i) {
        builder <<= 4;
        builder |= hexvalue(data[i]);
    }
    return builder;
}

// The signature will need to change with large values because we'll need to wipe out the old value (if it's larger and uses more keys).
MUST_USE ql::serialization_result_t
kv_location_set(
        FDBTransaction *txn, const std::string &kv_location,
        const ql::datum_t &data) {
    std::string str;
    ql::serialization_result_t res = datum_serialize_to_string(data, &str);
    if (bad(res)) {
        return res;
    }

    // Wipe out old value.
    std::string prefix = kv_prefix(kv_location);
    transaction_clear_prefix_range(txn, prefix);
    btubugf("kls %s\n", debug_str(prefix).c_str());

    // Write new value.
    size_t total_size = str.size();
    size_t num_parts;
    if (total_size == 0) {
        num_parts = 1;
    } else {
        num_parts = ceil_divide(total_size, LARGE_VALUE_SPLIT_SIZE);
    }

    // TODO: Declare this constant.  We have it because of the file format.
    guarantee(num_parts < 16384);  // OOO: Fail more gracefully.

    const size_t prefix_size = prefix.size();

    for (size_t i = 0; i < num_parts; ++i) {
        if (i == 0) {
            prefix.push_back(LARGE_VALUE_FIRST_PREFIX);
            append_bigendian_hex16(&prefix, num_parts);
        } else {
            prefix.push_back(LARGE_VALUE_SECOND_PREFIX);
            append_bigendian_hex16(&prefix, i);
        }
        btubugf("kls '%s', writing key '%s'\n", debug_str(prefix).c_str(), debug_str(key).c_str());
        const size_t front = i * LARGE_VALUE_SPLIT_SIZE;
        const size_t back = std::min(front + LARGE_VALUE_SPLIT_SIZE, str.size());
        transaction_set_buf(txn, prefix, str.data() + front, back - front);
        prefix.resize(prefix_size);
    }

    return res;
}

void kv_location_delete(
        FDBTransaction *txn, const std::string &kv_location) {
    std::string prefix = kv_prefix(kv_location);
    transaction_clear_prefix_range(txn, prefix);
}

rfdb::datum_fut kv_location_get(FDBTransaction *txn, const std::string &kv_location) {
    std::string lower_key = kv_prefix(kv_location);
    std::string upper_key = prefix_end(lower_key);

    const bool snapshot = false;

    return datum_fut{fdb_transaction_get_range(txn,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(lower_key.data()), int(lower_key.size())),
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper_key.data()), int(upper_key.size())),
        0,
        0,
        FDB_STREAMING_MODE_WANT_ALL,
        0,
        snapshot,
        false),
        lower_key,
        upper_key};
}

// TODO: Kind of redundant logic with transaction_read_whole_range_coro, except for the
// initial future.  Is this really helpful?

// Returns r_nullopt if there is no datum (which is a possible outcome of a datum_fut).
optional<std::vector<uint8_t>> block_and_read_unserialized_datum(
        FDBTransaction *txn,
        rfdb::datum_fut &&fut, const signal_t *interruptor) {
    // TODO: This is gross, use composition so that fut.block_coro() isn't called like
    // it is later.
    const std::string prefix = std::move(fut.prefix);
    btubugf("barud '%s'\n", debug_str(prefix).c_str());
    std::string begin = prefix;
    uint32_t counter = 0;
    uint32_t num_parts = 1;  // Unknown until counter > 0.  Set to 1 for convenience of
                             // later counter < num_parts guarantee.
    optional<std::vector<uint8_t>> ret;

    for (;;) {

        const FDBKeyValue *kvs = valgrind_undefined(nullptr);
        int kv_count = 0;
        fdb_bool_t more;

        fut.future.block_coro(interruptor);
        fdb_error_t err = fdb_future_get_keyvalue_array(fut.future.fut, &kvs, &kv_count, &more);
        check_for_fdb_transaction(err);
        btubugf("barud '%s' get kv array, count=%d\n", debug_str(prefix).c_str(), kv_count);

        for (int i = 0; i < kv_count; ++i) {
            guarantee(counter < num_parts);
            key_view key_slice{void_as_uint8(kvs[i].key), kvs[i].key_length};
            btubugf("barud '%s' see key '%s'\n", debug_str(prefix).c_str(),
                debug_str(key_slice.to_string()).c_str());
            key_view post_prefix = key_slice.guarantee_without_prefix(prefix);
            guarantee(post_prefix.length == LARGE_VALUE_SUFFIX_LENGTH);  // TODO: fdb, graceful, etc.
            static_assert(LARGE_VALUE_SUFFIX_LENGTH == 5, "bad suffix logic");
            uint32_t number = decode_bigendian_hex16(post_prefix.data + 1, 4);
            if (counter == 0) {
                guarantee(post_prefix.data[0] == LARGE_VALUE_FIRST_PREFIX);  // TODO: fdb, graceful, etc.
                num_parts = number;
                guarantee(num_parts > 0);
                ret.emplace();
            } else {
                guarantee(post_prefix.data[0] == LARGE_VALUE_SECOND_PREFIX);  // TODO: fdb, graceful, etc.
                guarantee(number == counter);  // TODO: fdb, graceful, etc.
            }

            ret->insert(ret->end(),
                void_as_uint8(kvs[i].value), void_as_uint8(kvs[i].value) + kvs[i].value_length);

            ++counter;
        }

        if (!more) {
            guarantee(counter == 0 || counter == num_parts);
            return ret;
        }

        guarantee(counter < num_parts);

        if (kv_count > 0) {
            const FDBKeyValue &kv = kvs[kv_count - 1];
            begin = std::string(void_as_char(kv.key), size_t(kv.key_length));
        }
        // TODO: er, maybe just deconstruct the param into local variables.
        fut.future = fdb_future{fdb_transaction_get_range(txn,
            FDB_KEYSEL_FIRST_GREATER_THAN(as_uint8(begin.data()), int(begin.size())),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(fut.upper_key.data()), int(fut.upper_key.size())),
            0,
            0,
            FDB_STREAMING_MODE_WANT_ALL,
            0,
            false,
            false)};
    }
}

datum_range_iterator primary_prefix_make_iterator(const std::string &pkey_prefix,
        const store_key_t &lower, const store_key_t *upper_or_null,
        fdb_bool_t snapshot, fdb_bool_t reverse) {
    // Remember we might be iterating backwards, so take care with these bounds.
    std::string lower_bound = kv_prefix(index_key_concat(pkey_prefix, lower));
    std::string upper_bound = upper_or_null
        ? kv_prefix(index_key_concat(pkey_prefix, *upper_or_null))
        : prefix_end(pkey_prefix);

    const uint32_t number = 0;

    return datum_range_iterator{
        std::move(pkey_prefix),
        std::move(lower_bound),
        std::move(upper_bound),
        snapshot,
        reverse,
        number,
        std::vector<std::vector<uint8_t>>() // partial_document_
    };
}

std::pair<std::vector<std::pair<store_key_t, std::vector<uint8_t>>>, bool>
datum_range_iterator::query_and_step(
        FDBTransaction *txn, const signal_t *interruptor, FDBStreamingMode mode) {
    const int limit = 0;
    const int target_bytes = 0;
    const int iteration = 0;

    btubugf("qas '%s'\n", debug_str(lower_).c_str());

    // TODO: We'll want roll-back/retry logic in place of "MEDIUM".
    fdb_future fut{fdb_transaction_get_range(txn,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(lower_.data()), int(lower_.size())),
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper_.data()), int(upper_.size())),
        limit,
        target_bytes,
        mode,
        iteration,
        snapshot_,
        reverse_)};

    fut.block_coro(interruptor);

    const FDBKeyValue *kvs;
    int kv_count;
    fdb_bool_t more;
    fdb_error_t err = fdb_future_get_keyvalue_array(fut.fut, &kvs, &kv_count, &more);
    check_for_fdb_transaction(err);

    std::pair<std::vector<std::pair<store_key_t, std::vector<uint8_t>>>, bool> ret;
    ret.second = more;

    // Initialized and used if kv_count > 0.  Is simply the last full_key.
    key_view last_key_view;

    // Initialized and used if kv_count > 0.
    for (int i = 0; i < kv_count; ++i) {
        key_view full_key{void_as_uint8(kvs[i].key), kvs[i].key_length};
        btubugf("qas '%s', key '%s'\n", debug_str(lower_).c_str(), debug_str(std::string(as_char(full_key.data), full_key.length)).c_str());
        key_view partial_key = full_key.guarantee_without_prefix(pkey_prefix_);
        last_key_view = full_key;
        // Now, we've got a primary key, followed by '\0', followed by 5 bytes.
        static_assert(LARGE_VALUE_SUFFIX_LENGTH == 5, "bad suffix logic");
        guarantee(partial_key.length >= 6);  // TODO: fdb, graceful, etc.
        guarantee(partial_key.data[partial_key.length - 6] == '\0');  // TODO: fdb, graceful
        uint8_t b = partial_key.data[partial_key.length - 5];
        guarantee(b == LARGE_VALUE_FIRST_PREFIX || b == LARGE_VALUE_SECOND_PREFIX);  // TODO: fdb, graceful
        uint32_t number = decode_bigendian_hex16(partial_key.data + (partial_key.length - 4), 4);

        // QQQ: We might sanity check that the key doesn't change.

        store_key_t the_key(partial_key.length - 6, partial_key.data);

        std::vector<uint8_t> buf{
            void_as_uint8(kvs[i].value), void_as_uint8(kvs[i].value) + size_t(kvs[i].value_length)};
        if (reverse_) {
            if (b == LARGE_VALUE_SECOND_PREFIX) {
                partial_document_.push_back(std::move(buf));
                guarantee(number_ == 0 || number == number_ - 1);  // TODO: fdb, graceful
                number_ = number;
            } else /* already checked b == '\x30' */ {
                // We sanity check document count.
                guarantee(number == 1 + partial_document_.size());  // TODO: fdb, graceful
                guarantee(number_ == (partial_document_.empty() ? 0 : 1));  // TODO: fdb, graceful
                number_ = 0;
                // TODO: Note we can deserialize datums from a buffer group.
                for (size_t j = partial_document_.size(); j-- > 0;) {
                    buf.insert(buf.end(), partial_document_[j].begin(), partial_document_[j].end());
                }
                partial_document_.clear();
                ret.first.emplace_back(std::move(the_key), std::move(buf));
            }
        } else {
            if (b == LARGE_VALUE_FIRST_PREFIX) {
                guarantee(number > 0);  // TODO: fdb, graceful
                guarantee(partial_document_.empty());  // TODO: fdb, graceful
                guarantee(number_ == 0);
                if (number == 1) {
                    // Skip pushing onto partial_document_.
                    ret.first.emplace_back(std::move(the_key), std::move(buf));
                } else {
                    partial_document_.push_back(std::move(buf));
                    number_ = number;
                }
            } else /* already checked b == '\x31' */ {
                guarantee(number == partial_document_.size());  // TODO: fdb, graceful
                partial_document_.push_back(std::move(buf));
                if (number_ == partial_document_.size()) {
                    // TODO: Note we can deserialize datums from a buffer group.
                    std::vector<uint8_t> builder = std::move(partial_document_[0]);
                    for (size_t j = 1; j < partial_document_.size(); ++j) {
                        builder.insert(builder.end(), partial_document_[j].begin(), partial_document_[j].end());
                    }
                    ret.first.emplace_back(std::move(the_key), std::move(builder));
                    number_ = 0;
                    partial_document_.clear();
                }
            }
        }
    }

    if (kv_count > 0) {
        if (reverse_) {
            upper_.assign(void_as_char(last_key_view.data), size_t(last_key_view.length));
        } else {
            lower_.assign(void_as_char(last_key_view.data), size_t(last_key_view.length));
            // Increment key.
            lower_.push_back('\0');
        }
    }

    return ret;
}


secondary_range_fut secondary_prefix_get_range(FDBTransaction *txn,
        const std::string &kv_prefix,
        const store_key_t &lower, lower_bound lower_bound_closed,
        const store_key_t *upper_or_null,
        int limit, int target_bytes, FDBStreamingMode mode, int iteration,
        fdb_bool_t snapshot, fdb_bool_t reverse) {
    std::string lower_key = index_key_concat(kv_prefix, lower);
    std::string upper_key = upper_or_null ? index_key_concat(kv_prefix, *upper_or_null)
        : prefix_end(kv_prefix);

    return secondary_range_fut{fdb_transaction_get_range(txn,
        as_uint8(lower_key.data()), int(lower_key.size()), lower_bound_closed == lower_bound::open, 1,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper_key.data()), int(upper_key.size())),
        limit,
        target_bytes,
        mode,
        iteration,
        snapshot,
        reverse)};
}

}  // namespace rfdb
