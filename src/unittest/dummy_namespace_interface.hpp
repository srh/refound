// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef UNITTEST_DUMMY_NAMESPACE_INTERFACE_HPP_
#define UNITTEST_DUMMY_NAMESPACE_INTERFACE_HPP_

#include <set>
#include <vector>

#include "containers/scoped.hpp"
#include "protocol_api.hpp"
#include "unittest/unittest_utils.hpp"  // TODO: Included only for RDB_FDB_UNITTEST

class rdb_context_t;

namespace unittest {

#if RDB_FDB_UNITTEST

class dummy_performer_t {

public:
    explicit dummy_performer_t(store_view_t *s) :
        store(s) { }

    store_view_t *store;
};

struct dummy_timestamper_t {

public:
    explicit dummy_timestamper_t(dummy_performer_t *n);

private:
    dummy_performer_t *next;
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

private:
    shard_t the_shard;
};

class dummy_namespace_interface_t : public namespace_interface_t {
public:
    dummy_namespace_interface_t(store_view_t *stores,
                                bool initialize_metadata);

    void read(UNUSED auth::user_context_t const &user_context,
              const read_t &_read,
              read_response_t *response,
              const signal_t *interruptor)
                  THROWS_ONLY(cannot_perform_query_exc_t,
                              interrupted_exc_t,
                              auth::permission_error_t) {
        return sharder->read(_read, response, tok, interruptor);
    }

    void write(UNUSED auth::user_context_t const &user_context,
               const write_t &_write,
               write_response_t *response,
               const signal_t *interruptor)
                   THROWS_ONLY(cannot_perform_query_exc_t,
                               interrupted_exc_t,
                               auth::permission_error_t) {
        return sharder->write(_write, response, tok, interruptor);
    }

private:
    // Just one performer and timestamper now.
    scoped_ptr_t<dummy_performer_t> the_performer;
    scoped_ptr_t<dummy_timestamper_t> the_timestamper;
    scoped_ptr_t<dummy_sharder_t> sharder;
};

#endif  // RDB_FDB_UNITTEST

}   /* namespace unittest */

#endif /* UNITTEST_DUMMY_NAMESPACE_INTERFACE_HPP_ */
