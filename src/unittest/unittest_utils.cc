// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "unittest/unittest_utils.hpp"

#include <stdlib.h>

#include <functional>

#include "arch/timing.hpp"
#include "arch/runtime/starter.hpp"
#include "random.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rdb_protocol/pseudo_time.hpp"
#include "unittest/gtest.hpp"
#include "utils.hpp"

namespace unittest {

std::string rand_string(int len) {
    std::string res;

    int seed = randint(RAND_MAX);

    while (len --> 0) {
        res.push_back((seed % 26) + 'A');
        seed ^= seed >> 17;
        seed += seed << 11;
        seed ^= seed >> 29;
    }

    return res;
}

struct make_sindex_read_t {
    static read_t make_sindex_read(ql::datum_t key, const std::string &id) {
        ql::datum_range_t rng(key, key_range_t::closed, key, key_range_t::closed);
        return read_t(
            rget_read_t(
#if RDB_CF
                optional<changefeed_stamp_t>(),
#endif
                region_t::universe(),
                r_nullopt,
                serializable_env_t{
                    ql::global_optargs_t(),
                    auth::user_context_t(auth::permissions_t(tribool::False, tribool::False, tribool::False, tribool::False)),
                    ql::datum_t()},
                "",
                ql::batchspec_t::default_for(ql::batch_type_t::NORMAL),
                std::vector<ql::transform_variant_t>(),
                optional<ql::terminal_variant_t>(),
                make_optional(sindex_rangespec_t(id,
                                                 r_nullopt,
                                                 ql::datumspec_t(rng),
                                                 require_sindexes_t::NO)),
                sorting_t::UNORDERED),
            profile_bool_t::PROFILE,
            read_mode_t::SINGLE);
    }
};

read_t make_sindex_read(
        ql::datum_t key, const std::string &id) {
    return make_sindex_read_t::make_sindex_read(key, id);
}

static const char *const temp_file_create_suffix = ".create";

temp_file_t::temp_file_t() {
#ifdef _WIN32
    char tmpl[MAX_PATH + 1];
    DWORD res = GetTempPath(MAX_PATH, tmpl);
    guarantee_winerr(res != 0, "GetTempPath failed");
    filename = std::string(tmpl) + strprintf("rdb_unittest.%6d", randint(1000000));
#else
    char tmpl[] = "/tmp/rdb_unittest.XXXXXX";
    for (;;) {
        const int fd = mkstemp(tmpl);
        guarantee_err(fd != -1, "Couldn't create a temporary file");
        close(fd);

        // Check that both the permanent and temporary file paths are unused.
        const std::string tmpfilename = std::string(tmpl) + temp_file_create_suffix;
        if (::access(tmpfilename.c_str(), F_OK) == -1 && get_errno() == ENOENT) {
            filename = tmpl;
            break;
        } else {
            const int unlink_res = ::unlink(tmpl);
            EXPECT_EQ(0, unlink_res);
        }
    }
#endif
}

temp_file_t::~temp_file_t() {
    // Unlink both possible locations of the file.
    const int res1 = ::unlink((filename + temp_file_create_suffix).c_str());
    EXPECT_TRUE(res1 == 0 || get_errno() == ENOENT);
    const int res2 = ::unlink(filename.c_str());
    EXPECT_TRUE(res2 == 0 || get_errno() == ENOENT);
}

temp_directory_t::temp_directory_t() {
#ifdef _WIN32
    char tmpl[] = "rdb_unittest.";
    char path[MAX_PATH + 1 + sizeof(tmpl) + 6 + 1];
    DWORD res = GetTempPath(sizeof(path), path);
    guarantee_winerr(res != 0 && res < MAX_PATH + 1, "GetTempPath failed");
    strcpy(path + res, tmpl); // NOLINT
    char *end = path + strlen(path);
    int tries = 0;
    while (true) {
        snprintf(end, 7, "%06d", randint(1000000)); // NOLINT(runtime/printf)
        BOOL res = CreateDirectory(path, nullptr);
        if (res) {
            directory = base_path_t(std::string(path));
            break;
        }
        if (GetLastError() == ERROR_ALREADY_EXISTS) {
            if (tries > 10) {
                guarantee_winerr(res, "CreateDirectory failed");
            }
            tries++;
            continue;
        }
        guarantee_winerr(res, "CreateDirectory failed");
    }
#else
    char tmpl[] = "/tmp/rdb_unittest.XXXXXX";
    char *res = mkdtemp(tmpl);
    guarantee_err(res != nullptr, "Couldn't create a temporary directory");
    directory = base_path_t(std::string(res));
#endif
}

temp_directory_t::~temp_directory_t() {
    remove_directory_recursive(directory.path().c_str());
}

base_path_t temp_directory_t::path() const {
    return directory;
}

void let_stuff_happen() {
#ifdef VALGRIND
    nap(2000);
#else
    nap(100);
#endif
}

std::set<ip_address_t> get_unittest_addresses() {
    return get_local_ips(std::set<ip_address_t>(),
                         local_ip_filter_t::MATCH_FILTER_OR_LOOPBACK);
}

void run_in_thread_pool(const std::function<void()> &fun, int num_workers) {
    ::run_in_thread_pool(fun, num_workers);
}

key_range_t quick_range(const char *bounds) {
    guarantee(strlen(bounds) == 3);
    char left = bounds[0];
    guarantee(bounds[1] == '-');
    char right = bounds[2];
    if (left != '*' && right != '*') {
        guarantee(left <= right);
    }
    key_range_t r;
    r.left = (left == '*')
        ? store_key_t()
        : store_key_t(std::string(1, left));
    r.right = (right == '*')
        ? key_range_t::right_bound_t()
        : key_range_t::right_bound_t(store_key_t(std::string(1, right+1)));
    return r;
}

region_t quick_region(const char *bounds) {
    return region_t(quick_range(bounds));
}

std::string random_letter_string(rng_t *rng, int min_length, int max_length) {
    std::string ret;
    int size = min_length + rng->randint(max_length - min_length + 1);
    for (int i = 0; i < size; i++) {
        ret.push_back('a' + rng->randint(26));
    }
    return ret;
}

// Zero extend the given byte to 64-bits.
uint64_t zero_extend(char x) {
    return static_cast<uint64_t>(static_cast<uint8_t>(x));
}

uint64_t decode_le64(const std::string& buf) {
    return zero_extend(buf[0]) |
        zero_extend(buf[1]) << 8 |
        zero_extend(buf[2]) << 16 |
        zero_extend(buf[3]) << 24 |
        zero_extend(buf[4]) << 32 |
        zero_extend(buf[5]) << 40 |
        zero_extend(buf[6]) << 48 |
        zero_extend(buf[7]) << 56;
}

uint32_t decode_le32(const std::string& buf) {
    return static_cast<uint32_t>(
        zero_extend(buf[0]) |
        zero_extend(buf[1]) << 8 |
        zero_extend(buf[2]) << 16 |
        zero_extend(buf[3]) << 32);
}

std::string encode_le64(uint64_t x) {
    char buf[8];
    buf[0] = x;
    buf[1] = x >> 8;
    buf[2] = x >> 16;
    buf[3] = x >> 24;
    buf[4] = x >> 32;
    buf[5] = x >> 40;
    buf[6] = x >> 48;
    buf[7] = x >> 56;
    return std::string(&buf[0], 8);
}

std::string encode_le32(uint32_t x) {
    char buf[4];
    buf[0] = x;
    buf[1] = x >> 8;
    buf[2] = x >> 16;
    buf[3] = x >> 24;
    return std::string(&buf[0], 4);
}

}  // namespace unittest
