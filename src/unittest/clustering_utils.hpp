// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef UNITTEST_CLUSTERING_UTILS_HPP_
#define UNITTEST_CLUSTERING_UTILS_HPP_

#include <functional>
#include <map>
#include <set>
#include <string>

#include "arch/io/disk.hpp"
#include "arch/timing.hpp"
#include "unittest/gtest.hpp"
#include "unittest/unittest_utils.hpp"
#include "random.hpp"
#include "rdb_protocol/protocol.hpp"

namespace unittest {

#if RDB_FDB_UNITTEST

class test_store_t {
public:
    test_store_t(io_backender_t *io_backender, rdb_context_t *ctx) :
            store(io_backender->rocks(),
                temp_file.name().permanent_path().c_str(), true,
                &get_global_perfmon_collection(), ctx, io_backender, base_path_t("."),
                namespace_id_t{generate_uuid()}, update_sindexes_t::UPDATE) {
        /* Initialize store metadata */
        cond_t non_interruptor;
        version_t new_metainfo = version_t::zero();
        store.set_metainfo(new_metainfo,
            write_durability_t::SOFT, &non_interruptor);
    }

    temp_file_t temp_file;
    store_t store;
};

class test_inserter_t {

public:
    typedef std::map<std::string, std::string> state_t;

    test_inserter_t(
            const std::string &_tag,
            state_t *state) :
        values_inserted(state), tag(_tag), next_value(0)
    {
        for (const auto &pair : *values_inserted) {
            keys_used.push_back(pair.first);
        }
    }

    virtual ~test_inserter_t() {
        guarantee(!running(), "subclass should call stop()");
    }

    void insert(size_t n) {
        while (n-- > 0) {
            insert_one();
        }
    }

    void start() {
        drainer.init(new auto_drainer_t);
        coro_t::spawn_sometime(std::bind(
            &test_inserter_t::insert_forever, this,
            auto_drainer_t::lock_t(drainer.get())));
    }

    void stop() {
        rassert(drainer.has());
        drainer.reset();
    }

    bool running() const {
        return drainer.has();
    }

    void validate() {
        for (state_t::iterator it = values_inserted->begin();
                               it != values_inserted->end();
                               it++) {
            cond_t non_interruptor;
            std::string response = read(
                it->first,
                &non_interruptor);
            if (it->second != response) {
                report_error(it->first, it->second, response);
            }
        }
    }

    /* `validate_no_extras()` makes sure that no keys from `extras` are present that are
    not supposed to be present. It complements `validate`, which only checks for the
    existence of keys that are supposed to exist. */
    void validate_no_extras(const std::map<std::string, std::string> &extras) {
        for (const auto &pair : extras) {
            auto it = values_inserted->find(pair.first);
            std::string expect = it != values_inserted->end() ? it->second : "";
            cond_t non_interruptor;
            std::string actual = read(
                pair.first,
                &non_interruptor);
            if (expect != actual) {
                report_error(pair.first, expect, actual);
            }
        }
    }

    state_t *values_inserted;

protected:
    virtual void write(
            const std::string &, const std::string &, const signal_t *) = 0;
    virtual std::string read(const std::string &, const signal_t *) = 0;
    virtual std::string generate_key() = 0;

    virtual void report_error(
            const std::string &key,
            const std::string &expect,
            const std::string &actual) {
        crash("For key `%s`: expected `%s`, got `%s`\n",
            key.c_str(), expect.c_str(), actual.c_str());
    }

private:
    scoped_ptr_t<auto_drainer_t> drainer;

    void insert_one() {
        std::string key, value;
        if (randint(3) != 0 || keys_used.empty()) {
            key = generate_key();
            value = strprintf("%d", next_value++);
            keys_used.push_back(key);
        } else {
            key = keys_used.at(randint(keys_used.size()));
            if (randint(2) == 0) {
                /* `wfun()` may choose to interpret this as a deletion */
                value = "";
            } else {
                value = strprintf("%d", next_value++);
            }
        }
        (*values_inserted)[key] = value;
        cond_t interruptor;
        write(key, value, &interruptor);
    }

    void insert_forever(auto_drainer_t::lock_t keepalive) {
        try {
            for (;;) {
                if (keepalive.get_drain_signal()->is_pulsed()) throw interrupted_exc_t();
                insert_one();
                nap(10, keepalive.get_drain_signal());
            }
        } catch (const interrupted_exc_t &) {
            /* Break out of loop */
        }
    }

    std::vector<std::string> keys_used;
    std::string tag;
    int next_value;

    DISABLE_COPYING(test_inserter_t);
};

inline std::string alpha_key_gen(int len) {
    std::string key;
    for (int j = 0; j < len; j++) {
        key.push_back('a' + randint(26));
    }
    return key;
}

inline std::string dummy_key_gen() {
    return alpha_key_gen(1);
}

inline std::string mc_key_gen() {
    return alpha_key_gen(100);
}

#endif  // RDB_FDB_UNITTEST

}  // namespace unittest

#endif  // UNITTEST_CLUSTERING_UTILS_HPP_
