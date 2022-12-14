// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "btree/keys.hpp"

#include <algorithm>

#include "debug.hpp"
#include "math.hpp"
#include "utils.hpp"

bool store_key_t::increment() {
    if (str_.size() < MAX_KEY_SIZE) {
        str_.push_back(0);
        return true;
    }
    while (str_.size() > 0 && static_cast<uint8_t>(str_.back()) == 255) {
        str_.pop_back();
    }
    if (str_.empty()) {
        /* We were the largest possible key. Oops. Restore our previous
        state and return `false`. */
        str_.resize(MAX_KEY_SIZE, char(255));
        return false;
    }
    str_.back() = 1 + static_cast<uint8_t>(str_.back());
    return true;
}

bool store_key_t::increment1() {
    if (str_.size() < MAX_KEY_SIZE) {
        str_.push_back(1);
        return true;
    }
    while (str_.size() > 0 && static_cast<uint8_t>(str_.back()) == 255) {
        str_.pop_back();
    }
    if (str_.empty()) {
        /* We were the largest possible key. Oops. Restore our previous
        state and return `false`. */
        str_.resize(MAX_KEY_SIZE, char(255));
        return false;
    }
    str_.back() = 1 + static_cast<uint8_t>(str_.back());
    return true;
}

// The wire format of serialize_for_metainfo must not change.
void store_key_t::serialize_for_metainfo(write_message_t *wm) const {
    uint8_t sz = size();
    serialize_universal(wm, sz);
    wm->append(data(), sz);
}

archive_result_t store_key_t::deserialize_for_metainfo(read_stream_t *s) {
    uint8_t sz;
    archive_result_t res = deserialize_universal(s, &sz);
    if (bad(res)) { return res; }
    std::string buf;
    buf.resize(sz);
    int64_t num_read = force_read(s, &buf[0], sz);
    if (num_read == -1) {
        return archive_result_t::SOCK_ERROR;
    }
    if (num_read < sz) {
        return archive_result_t::SOCK_EOF;
    }
    rassert(num_read == sz);
    str_ = std::move(buf);
    return archive_result_t::SUCCESS;
}


std::string key_range_t::print() const {
    printf_buffer_t buf;
    debug_print(&buf, *this);
    return buf.c_str();
}

bool unescaped_str_to_key(const char *str, int len, store_key_t *buf) {
    if (len <= MAX_KEY_SIZE) {
        *buf = store_key_t(std::string(str, len));
        return true;
    } else {
        return false;
    }
}

// QQQ: Remove this!
std::string key_to_unescaped_str(const store_key_t &key) {
    return key.str();
}

std::string key_to_debug_str(const store_key_t &key) {
    std::string s;
    s.push_back('"');
    for (int i = 0; i < key.size(); i++) {
        uint8_t c = key.data()[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
            s.push_back(c);
        } else {
            s.push_back('\\');
            s.push_back('x');
            s.push_back(int_to_hex((c & 0xf0) >> 4));
            s.push_back(int_to_hex(c & 0x0f));
        }
    }
    s.push_back('"');
    return s;
}

key_range_t::key_range_t() :
    left(), right(store_key_t()) { }

key_range_t::key_range_t(bound_t lm, const store_key_t& l, bound_t rm, const store_key_t& r) {
    init(lm, store_key_t(l), rm, store_key_t(r));
}

key_range_t::key_range_t(bound_t lm, store_key_t&& l, bound_t rm, store_key_t&& r) {
    init(lm, std::move(l), rm, std::move(r));
}


void key_range_t::init(bound_t lm, store_key_t &&l, bound_t rm, store_key_t &&r) {
    switch (lm) {
        case bound_t::closed:
            left = std::move(l);
            break;
        case bound_t::open:
            left = std::move(l);
            if (left.increment1()) {
                break;
            } else {
                rassert(rm == bound_t::none);
                /* Our left bound is the largest possible key, and we are open
                on the left-hand side. So we are empty. */
                *this = key_range_t::empty();
                return;
            }
        case bound_t::none:
            left = store_key_t::min();
            break;
        default:
            unreachable();
    }

    switch (rm) {
        case bound_t::closed: {
            right.unbounded = false;
            right.key() = std::move(r);
            bool ok = right.increment1();
            guarantee(ok);
            break;
        }
        case bound_t::open:
            right.unbounded = false;
            right.key() = std::move(r);
            break;
        case bound_t::none:
            right.unbounded = true;
            break;
        default:
            unreachable();
    }

    rassert(right.unbounded || left <= right.key(),
            "left_key(%d)=%s, right_key(%d)=%s",
            left.size(),
            key_to_debug_str(left).c_str(),
            right.internal_key.size(),
            key_to_debug_str(right.internal_key).c_str());
}

bool key_range_t::is_superset(const key_range_t &other) const {
    /* Special-case empty ranges */
    if (other.is_empty()) return true;
    if (left > other.left) return false;
    if (right < other.right) return false;
    return true;
}

bool key_range_t::overlaps(const key_range_t &other) const {
    return key_range_t::right_bound_t(left) < other.right &&
        key_range_t::right_bound_t(other.left) < right &&
        !is_empty() && !other.is_empty();
}

key_range_t key_range_t::intersection(const key_range_t &other) const {
    if (!overlaps(other)) {
        return key_range_t::empty();
    }
    key_range_t ixn;
    ixn.left = left < other.left ? other.left : left;
    ixn.right = right > other.right ? other.right : right;
    return ixn;
}

void debug_print(printf_buffer_t *buf, const store_key_t &k) {
    debug_print_quoted_string(buf, k.data(), k.size());
}

void debug_print(printf_buffer_t *buf, const key_range_t::right_bound_t &rb) {
    if (rb.unbounded) {
        buf->appendf("+inf");
    } else {
        debug_print(buf, rb.key());
    }
}

void debug_print(printf_buffer_t *buf, const key_range_t &kr) {
    buf->appendf("[");
    debug_print(buf, kr.left);
    buf->appendf(", ");
    debug_print(buf, kr.right);
    buf->appendf(")");
}

std::string key_range_to_string(const key_range_t &kr) {
    std::string res;
    res += "[";
    res += key_to_debug_str(kr.left);
    res += ", ";
    if (kr.right.unbounded) {
        res += "+inf";
    } else {
        res += key_to_debug_str(kr.right.key());
    }
    res += ")";
    return res;
}

void debug_print(printf_buffer_t *buf, const store_key_t *k) {
    if (k) {
        debug_print(buf, *k);
    } else {
        buf->appendf("NULL");
    }
}


bool operator==(const key_range_t::right_bound_t &a, const key_range_t::right_bound_t &b) {
    return (a.unbounded && b.unbounded)
        || (!a.unbounded && !b.unbounded && a.key() == b.key());
}
bool operator!=(const key_range_t::right_bound_t &a, const key_range_t::right_bound_t &b) {
    return !(a == b);
}
bool operator<(const key_range_t::right_bound_t &a, const key_range_t::right_bound_t &b) {
    if (a.unbounded) return false;
    if (b.unbounded) return true;
    return a.key() < b.key();
}
bool operator<=(const key_range_t::right_bound_t &a, const key_range_t::right_bound_t &b) {
    return a == b || a < b;
}
bool operator>(const key_range_t::right_bound_t &a, const key_range_t::right_bound_t &b) {
    return b < a;
}
bool operator>=(const key_range_t::right_bound_t &a, const key_range_t::right_bound_t &b) {
    return b <= a;
}

bool operator==(key_range_t a, key_range_t b) THROWS_NOTHING {
    return a.left == b.left && a.right == b.right;
}

bool operator!=(key_range_t a, key_range_t b) THROWS_NOTHING {
    return !(a == b);
}

bool operator<(const key_range_t &a, const key_range_t &b) THROWS_NOTHING {
    return (a.left < b.left || (a.left == b.left && a.right < b.right));
}

RDB_IMPL_SERIALIZABLE_2_SINCE_v1_13(key_range_t::right_bound_t, unbounded, internal_key);
RDB_IMPL_SERIALIZABLE_2_SINCE_v1_13(key_range_t, left, right);

void serialize_for_metainfo(write_message_t *wm, const key_range_t &kr) {
    kr.left.serialize_for_metainfo(wm);
    serialize_universal(wm, kr.right.unbounded);
    kr.right.internal_key.serialize_for_metainfo(wm);
}
archive_result_t deserialize_for_metainfo(read_stream_t *s, key_range_t *out) {
    archive_result_t res = out->left.deserialize_for_metainfo(s);
    if (bad(res)) { return res; }
    res = deserialize_universal(s, &out->right.unbounded);
    if (bad(res)) { return res; }
    return out->right.internal_key.deserialize_for_metainfo(s);
}
