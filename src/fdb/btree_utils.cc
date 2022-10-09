#include "fdb/btree_utils.hpp"

#include "debug.hpp"
#include "math.hpp"
#include "rdb_protocol/btree.hpp"

// TODO: Remove.
#define btubugf(...)
// #define btubugf(...) debugf(__VA_ARGS__)

namespace rfdb {

static constexpr size_t LARGE_VALUE_SPLIT_SIZE = 16384;
static constexpr char LARGE_VALUE_FIRST_PREFIX = '\x30';
static constexpr char LARGE_VALUE_SECOND_PREFIX = '\x31';

// large value suffix (which is preceded by the nul character terminating the primary key)
// (a) (1-byte) prefix char, either LARGE_VALUE_FIRST_PREFIX or LARGE_VALUE_SECOND_PREFIX
// (b) (8-bytes) 8-digit hex value (representing a 32-bit int) representing a size
// (c) (16-bytes) 16-digit hex value (representing a unique 64-bit int)
static constexpr size_t LARGE_VALUE_SUFFIX_LENGTH = 1 + 8 + 16;

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


void append_bigendian_hex32(std::string *str, uint32_t val) {
    push_hex(str, val >> 24);
    push_hex(str, val >> 16);
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

uint32_t decode_bigendian_hex32(const uint8_t *data, size_t length) {
    guarantee(length == 8);
    uint32_t builder = 0;
    for (size_t i = 0; i < 8; ++i) {
        builder <<= 4;
        builder |= hexvalue(data[i]);
    }
    return builder;
}

// Generates 16-byte hex string as that is the pkey format
unique_pkey_suffix generate_unique_pkey_suffix() {
    unique_pkey_suffix ret;
    randbuf128 identifier = generate_randbuf128();
    // 8 binary bytes -> 16 hex digits
    static_assert(sizeof(ret.data) == 16, "Expecting 16 hex bytes");
    for (size_t i = 0; i < 8; ++i) {
        std::pair<char, char> p = convert_hex8(identifier.data[i]);
        ret.data[i * 2] = p.first;
        ret.data[i * 2 + 1] = p.second;
    }
    return ret;
}

// The signature will need to change with large values because we'll need to wipe out the old value (if it's larger and uses more keys).
approx_txn_size kv_location_set(
        FDBTransaction *txn, const std::string &kv_location,
        const std::string &str /* datum_serialize_to_string result */) {
    // TODO: It would be cool if we took a write_message_t instead of a str.  It would
    // also be cool if the write_message_t chunks were sized by LARGE_VALUE_SPLIT_SIZE.

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
    guarantee(num_parts < 1048576);  // OOO: Fail more gracefully.

    const size_t prefix_size = prefix.size();
    unique_pkey_suffix unique_suffix = generate_unique_pkey_suffix();

    approx_txn_size ret{0};

    for (size_t i = 0; i < num_parts; ++i) {
        static_assert(25 == LARGE_VALUE_SUFFIX_LENGTH, "Expecting 1 + 8 + 16 pkey format");
        if (i == 0) {
            prefix.push_back(LARGE_VALUE_FIRST_PREFIX);
            append_bigendian_hex32(&prefix, num_parts);
        } else {
            prefix.push_back(LARGE_VALUE_SECOND_PREFIX);
            append_bigendian_hex32(&prefix, i);
        }
        prefix.append(unique_suffix.data, unique_suffix.size);

        btubugf("kls '%s', writing key '%s'\n", debug_str(prefix).c_str(), debug_str(key).c_str());
        const size_t front = i * LARGE_VALUE_SPLIT_SIZE;
        const size_t back = std::min(front + LARGE_VALUE_SPLIT_SIZE, str.size());
        const size_t sz = back - front;
        transaction_set_buf(txn, prefix, str.data() + front, sz);
        ret.value += prefix.size() + sz;

        prefix.resize(prefix_size);
    }

    return ret;
}

// Returns vague txn size.
approx_txn_size kv_location_delete(
        FDBTransaction *txn, const std::string &kv_location) {
    std::string prefix = kv_prefix(kv_location);
    return transaction_clear_prefix_range(txn, prefix);
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

        unique_pkey_suffix expected_identifier;  // Initialized on 0th iteration.
        for (int i = 0; i < kv_count; ++i) {
            guarantee(counter < num_parts);
            key_view key_slice{void_as_uint8(kvs[i].key), kvs[i].key_length};
            btubugf("barud '%s' see key '%s'\n", debug_str(prefix).c_str(),
                debug_str(key_slice.to_string()).c_str());
            key_view post_prefix = key_slice.guarantee_without_prefix(prefix);
            guarantee(post_prefix.length == LARGE_VALUE_SUFFIX_LENGTH);  // TODO: fdb, graceful, etc.
            static_assert(LARGE_VALUE_SUFFIX_LENGTH == 25, "bad suffix logic");
            uint32_t number = decode_bigendian_hex32(post_prefix.data + 1, 8);
            if (counter == 0) {
                guarantee(post_prefix.data[0] == LARGE_VALUE_FIRST_PREFIX);  // TODO: fdb, graceful, etc.
                num_parts = number;
                guarantee(num_parts > 0);
                ret.emplace();
            } else {
                guarantee(post_prefix.data[0] == LARGE_VALUE_SECOND_PREFIX);  // TODO: fdb, graceful, etc.
                guarantee(number == counter);  // TODO: fdb, graceful, etc.
            }
            unique_pkey_suffix identifier = unique_pkey_suffix::copy(post_prefix.data + 9, 16);
            if (i == 0) {
                expected_identifier = identifier;
            } else {
                guarantee(0 == expected_identifier.compare(identifier));
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

std::string datum_range_lower_bound(const std::string &pkey_prefix, const store_key_t &lower) {
    return kv_prefix(index_key_concat(pkey_prefix, lower));
}

std::string datum_range_upper_bound(const std::string &pkey_prefix, const store_key_t *upper_or_null) {
    return upper_or_null != nullptr
        ? kv_prefix(index_key_concat(pkey_prefix, *upper_or_null))
        : prefix_end(pkey_prefix);
}

datum_range_iterator primary_prefix_make_iterator(const std::string &pkey_prefix,
        const store_key_t &lower, const store_key_t *upper_or_null,
        fdb_bool_t snapshot, fdb_bool_t reverse) {
    // Remember we might be iterating backwards, so take care with these bounds.
    std::string lower_bound = datum_range_lower_bound(pkey_prefix, lower);
    std::string upper_bound = datum_range_upper_bound(pkey_prefix, upper_or_null);

    const uint32_t number = 0;

    return datum_range_iterator{
        std::move(pkey_prefix),
        std::move(lower_bound),
        std::move(upper_bound),
        snapshot,
        reverse,
        number,
        std::vector<std::vector<uint8_t>>(), // partial_document_
        false,
    };
}

std::pair<std::vector<std::pair<store_key_t, std::vector<uint8_t>>>, bool>
datum_range_iterator::query_and_step(
        FDBTransaction *txn, const signal_t *interruptor, FDBStreamingMode mode,
        const int target_bytes,
        size_t *const bytes_read_out) {
    const int limit = 0;
    const int iteration = 0;

    btubugf("qas '%s'\n", debug_str(lower_).c_str());

    // The retry label is for the case where a partially read document got misread.  We
    // can only retry exactly once, because split_across_txns_ gets flipped to false.
    // (Otherwise, we would return an empty read to the caller.)
 retry:

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

    size_t bytes_read_count = 0;

    for (int i = 0; i < kv_count; ++i) {
        bytes_read_count += kvs[i].key_length;
        bytes_read_count += kvs[i].value_length;

        key_view full_key{void_as_uint8(kvs[i].key), kvs[i].key_length};
        btubugf("qas '%s', key '%s'\n", debug_str(lower_).c_str(), debug_str(std::string(as_char(full_key.data), full_key.length)).c_str());
        key_view partial_key = full_key.guarantee_without_prefix(pkey_prefix_);
        last_key_view = full_key;
        // Now, we've got a primary key, followed by '\0', followed by 9 bytes.
        static_assert(LARGE_VALUE_SUFFIX_LENGTH == 25, "bad suffix logic");
        guarantee(partial_key.length >= 26);  // TODO: fdb, graceful, etc.
        guarantee(partial_key.data[partial_key.length - 26] == '\0');  // TODO: fdb, graceful
        uint8_t b = partial_key.data[partial_key.length - 25];
        guarantee(b == LARGE_VALUE_FIRST_PREFIX || b == LARGE_VALUE_SECOND_PREFIX);  // TODO: fdb, graceful
        uint32_t number = decode_bigendian_hex32(partial_key.data + (partial_key.length - 24), 8);
        unique_pkey_suffix identifier = unique_pkey_suffix::copy(partial_key.data + (partial_key.length - 16), 16);

        // QQQ: We might sanity check that the key doesn't change.

        store_key_t the_key(partial_key.length - 26, partial_key.data);

        std::vector<uint8_t> buf{
            void_as_uint8(kvs[i].value), void_as_uint8(kvs[i].value) + size_t(kvs[i].value_length)};
        if (reverse_) {
            if (b == LARGE_VALUE_SECOND_PREFIX) {
                if (partial_document_.empty()) {
                    last_seen_suffix_ = identifier;
                } else {
                    if (0 != last_seen_suffix_.compare(identifier)) {
                        if (split_across_txns_) {
                            guarantee(ret.first.empty());
                            std::string tmp = upper_;
                            guarantee(tmp.size() > LARGE_VALUE_SUFFIX_LENGTH);
                            tmp.resize(tmp.size() - LARGE_VALUE_SUFFIX_LENGTH);
                            upper_ = prefix_end(tmp);
                            partial_document_.clear();
                            split_across_txns_ = false;
                            goto retry;
                        } else {
                            crash("Non-matching identifier suffix on pkey");  // TODO: fdb, graceful
                        }
                    }
                }

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
                    last_seen_suffix_ = identifier;
                }
            } else /* already checked b == '\x31' */ {
                guarantee(number == partial_document_.size());  // TODO: fdb, graceful
                partial_document_.push_back(std::move(buf));
                if (0 != last_seen_suffix_.compare(identifier)) {
                    if (split_across_txns_) {
                        // We add 1 to the suffix length in this case (when chopping it
                        // off lower_) because we have lower_.push_back('\0') and we just
                        // want to set lower_ to the kv_prefix of our primary key.
                        guarantee(lower_.size() > LARGE_VALUE_SUFFIX_LENGTH + 1);
                        guarantee(lower_.back() == '\0');
                        lower_.resize(lower_.size() - (LARGE_VALUE_SUFFIX_LENGTH + 1));
                        partial_document_.clear();
                        split_across_txns_ = false;
                        goto retry;
                    } else {
                        crash("Non-matching identifier suffix on pkey");  // TODO: fdb, graceful
                    }
                }
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
        split_across_txns_ = false;
    }

    if (kv_count > 0) {
        if (reverse_) {
            upper_.assign(void_as_char(last_key_view.data), size_t(last_key_view.length));
        } else {
            lower_.assign(void_as_char(last_key_view.data), size_t(last_key_view.length));
            // Increment key.
            lower_.push_back('\0');
        }
    } else {
        // It is possible that a partial document got rewritten as we tried to read it.
        if (!more && !partial_document_.empty()) {
            if (split_across_txns_) {
                // These two braches have identical logic as the above retry branches.
                if (reverse_) {
                    std::string tmp = upper_;
                    guarantee(tmp.size() > LARGE_VALUE_SUFFIX_LENGTH);
                    tmp.resize(tmp.size() - LARGE_VALUE_SUFFIX_LENGTH);
                    upper_ = prefix_end(tmp);
                    partial_document_.clear();
                    split_across_txns_ = false;
                    goto retry;
                } else {
                    // We add 1 to the suffix length in this case (when chopping it
                    // off lower_) because we have lower_.push_back('\0') and we just
                    // want to set lower_ to the kv_prefix of our primary key.
                    guarantee(lower_.size() > LARGE_VALUE_SUFFIX_LENGTH + 1);
                    lower_.resize(lower_.size() - (LARGE_VALUE_SUFFIX_LENGTH + 1));
                    partial_document_.clear();
                    split_across_txns_ = false;
                    goto retry;
                }
            } else {
                crash("Read hit end-of-range mid-document");  // TODO: fdb, graceful
            }
        }
    }

    if (bytes_read_out != nullptr) {
        *bytes_read_out = bytes_read_count;
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
