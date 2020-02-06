// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef UNITTEST_DUMMY_NAMESPACE_INTERFACE_HPP_
#define UNITTEST_DUMMY_NAMESPACE_INTERFACE_HPP_

#include <set>
#include <vector>

#include "containers/scoped.hpp"
#include "protocol_api.hpp"
#include "timestamps.hpp"

class rdb_context_t;

namespace unittest {

class dummy_performer_t {

public:
    explicit dummy_performer_t(store_view_t *s) :
        store(s) { }

    void read(const read_t &read,
              read_response_t *response,
              DEBUG_VAR state_timestamp_t expected_timestamp,
              signal_t *interruptor) THROWS_ONLY(interrupted_exc_t);

    void read_outdated(const read_t &read,
                       read_response_t *response,
                       signal_t *interruptor) THROWS_ONLY(interrupted_exc_t);

    void write(const write_t &write,
               write_response_t *response,
               state_timestamp_t timestamp,
               order_token_t order_token) THROWS_NOTHING;

    order_source_t bs_outdated_read_source;

    store_view_t *store;
};

struct dummy_timestamper_t {

public:
    dummy_timestamper_t(dummy_performer_t *n, order_source_t *order_source);

    void read(const read_t &read, read_response_t *response, order_token_t otok, signal_t *interruptor) THROWS_ONLY(interrupted_exc_t);

    void write(const write_t &write, write_response_t *response, order_token_t otok) THROWS_NOTHING;
private:
    dummy_performer_t *next;
    state_timestamp_t current_timestamp;
    order_sink_t order_sink;
};

class dummy_sharder_t {

public:
    struct shard_t {
        shard_t(dummy_timestamper_t *ts, dummy_performer_t *pf) :
            timestamper(ts), performer(pf) { }
        dummy_timestamper_t *timestamper;
        dummy_performer_t *performer;
        // Region is universe.
    };

    explicit dummy_sharder_t(shard_t &&_the_shard)
        : the_shard(std::move(_the_shard)) { }

    void read(const read_t &read, read_response_t *response, order_token_t tok, signal_t *interruptor);

    void write(const write_t &write, write_response_t *response, order_token_t tok, signal_t *interruptor);

private:
    shard_t the_shard;
};

class dummy_namespace_interface_t : public namespace_interface_t {
public:
    dummy_namespace_interface_t(store_view_t *stores, order_source_t
                                *order_source,
                                bool initialize_metadata);

    void read(UNUSED auth::user_context_t const &user_context,
              const read_t &_read,
              read_response_t *response,
              order_token_t tok,
              signal_t *interruptor)
                  THROWS_ONLY(cannot_perform_query_exc_t,
                              interrupted_exc_t,
                              auth::permission_error_t) {
        return sharder->read(_read, response, tok, interruptor);
    }

    void write(UNUSED auth::user_context_t const &user_context,
               const write_t &_write,
               write_response_t *response,
               order_token_t tok,
               signal_t *interruptor)
                   THROWS_ONLY(cannot_perform_query_exc_t,
                               interrupted_exc_t,
                               auth::permission_error_t) {
        return sharder->write(_write, response, tok, interruptor);
    }

    bool check_readiness(table_readiness_t, signal_t *) {
        throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
    }

private:
    // Just one performer and timestamper now.
    scoped_ptr_t<dummy_performer_t> the_performer;
    scoped_ptr_t<dummy_timestamper_t> the_timestamper;
    scoped_ptr_t<dummy_sharder_t> sharder;
};

}   /* namespace unittest */

#endif /* UNITTEST_DUMMY_NAMESPACE_INTERFACE_HPP_ */
