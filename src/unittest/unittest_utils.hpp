// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef UNITTEST_UNITTEST_UTILS_HPP_
#define UNITTEST_UNITTEST_UTILS_HPP_

#include <functional>
#include <set>
#include <string>

#include "arch/address.hpp"
#include "arch/timing.hpp"
#include "containers/scoped.hpp"
#include "fdb/fdb.hpp"
#include "paths.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rpc/serialize_macros.hpp"

// QQQ: Refactor non-compiling unit tests for fdb.
// Used to comment out unittests that use broken non-fdb interfaces -- until the code
// stabilizes (and has a remote chance of passing).
#define RDB_FDB_UNITTEST 0

class rng_t;

namespace unittest {

// Vestigial constants from hash-sharding, that some old unit test code still
// names.
static constexpr int THE_CPU_SHARD = 0;
static constexpr int CPU_SHARDING_FACTOR = 1;

// We'll either have to connect to fdb to run unittests (which is probably fine) or mock
// it.
inline FDBDatabase *TODO_fdb() {
    return nullptr;
}

inline void nap(int64_t ms) {
    cond_t non_interruptor;
    nap(ms, &non_interruptor);
}


std::string rand_string(int len);

class temp_file_t {
public:
    temp_file_t();
    ~temp_file_t();

private:
    std::string filename;

    DISABLE_COPYING(temp_file_t);
};

class temp_directory_t {
public:
    temp_directory_t();
    ~temp_directory_t();
    base_path_t path() const;

private:
    base_path_t directory;

    DISABLE_COPYING(temp_directory_t);
};

void let_stuff_happen();

std::set<ip_address_t> get_unittest_addresses();

void run_in_thread_pool(const std::function<void()> &fun, int num_workers = 1);

read_t make_sindex_read(
    ql::datum_t key, const std::string &id);

/* Easy way to make shard ranges. Examples:
 - `quick_range("A-Z")` contains any key starting with a capital letter.
 - `quick_range("A-M")` includes "Aardvark" and "Mammoth" but not "Quail".
 - `quick_range("*-*")` is `key_range_t::universe()`.
 - `quick_range("*-M")` and `quick_range("N-*")` are adjacent but do not overlap.
The `quick_region()` variant just wraps the `key_range_t` in a `region_t`. */
key_range_t quick_range(const char *bounds);
region_t quick_region(const char *bounds);

std::string random_letter_string(rng_t *rng, int min_length, int max_length);

// Read an 8-byte little-endian encoded integer from the given string.
uint64_t decode_le64(const std::string& buf);

// Read a 4-byte little-endian encoded integer from the given string.
uint32_t decode_le32(const std::string& buf);

// Write an 8-byte integer to a string in little-endian byte order.
std::string encode_le64(uint64_t x);

// Write a 4-byte integer to a string in little-endian byte order.
std::string encode_le32(uint32_t x);

}  // namespace unittest


#define TPTEST(group, name, ...) void run_##group##_##name();           \
    TEST(group, name) {                                                 \
        ::unittest::run_in_thread_pool(run_##group##_##name, ##__VA_ARGS__);      \
    }                                                                   \
    void run_##group##_##name()

#define TPTEST_MULTITHREAD(group, name, j) void run_##group##_##name(); \
    TEST(group, name##MultiThread) {                                    \
        ::unittest::run_in_thread_pool(run_##group##_##name, j);        \
    }                                                                   \
    TPTEST(group, name)

#endif /* UNITTEST_UNITTEST_UTILS_HPP_ */
