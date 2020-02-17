#ifndef UNITTEST_MOCK_STORE_HPP_
#define UNITTEST_MOCK_STORE_HPP_

#include <map>
#include <utility>
#include <string>

#include "clustering/immediate_consistency/version.hpp"
#include "rdb_protocol/protocol.hpp"
#include "store_view.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

write_t mock_overwrite(std::string key, std::string value);

read_t mock_read(std::string key);

std::string mock_parse_read_response(const read_response_t &rr);

#if RDB_FDB_UNITTEST
std::string mock_lookup(store_view_t *store, std::string key);
#endif  // RDB_FDB_UNITTEST

class mock_store_t : public store_view_t {
public:
    explicit mock_store_t(version_t universe_metainfo = version_t::zero());
    ~mock_store_t();
    void rethread(threadnum_t new_thread) {
        home_thread_mixin_t::real_home_thread = new_thread;
        token_source_.rethread(new_thread);
        token_sink_.rethread(new_thread);
        order_sink_.rethread(new_thread);
    }

#if RDB_CF
    void note_reshard() override { }
#endif

    void read(
            DEBUG_ONLY(const metainfo_checker_t &metainfo_checker, )
            const read_t &read,
            read_response_t *response,
            read_token_t *token,
            const signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t);

    void write(
            DEBUG_ONLY(const metainfo_checker_t &metainfo_checker, )
            const version_t &new_metainfo,
            const write_t &write,
            write_response_t *response,
            write_durability_t durability,
            state_timestamp_t timestamp,
            order_token_t order_token,
            write_token_t *token,
            const signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t);

    // Used by unit tests that expected old-style stuff.
    std::string values(std::string key);
    repli_timestamp_t timestamps(std::string key);

private:
    fifo_enforcer_source_t token_source_;
    fifo_enforcer_sink_t token_sink_;

    order_sink_t order_sink_;

    version_t metainfo_;
    std::map<store_key_t, std::pair<repli_timestamp_t, ql::datum_t> > table_;

    DISABLE_COPYING(mock_store_t);
};

}  // namespace unittest

#endif  // UNITTEST_MOCK_STORE_HPP_
