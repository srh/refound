#include "unittest/dummy_namespace_interface.hpp"

#include "unittest/clustering_utils.hpp"
#include "rdb_protocol/env.hpp"

namespace unittest {

void dummy_performer_t::write(const write_t &_write,
                              write_response_t *response,
                              state_timestamp_t timestamp,
                              order_token_t order_token) THROWS_NOTHING {
    cond_t non_interruptor;

#ifndef NDEBUG
    metainfo_checker_t metainfo_checker(region_t::universe(),
        [](const region_t &, const version_t &) { });
#endif

    write_token_t token;
    store->new_write_token(&token);

    store->write(
            DEBUG_ONLY(metainfo_checker, )
            region_map_t<version_t>(region_t::universe(), version_t(branch_id_t{uuid_u()}, timestamp)),
            _write, response, write_durability_t::SOFT, timestamp, order_token, &token, &non_interruptor);
}


dummy_timestamper_t::dummy_timestamper_t(dummy_performer_t *n,
                                         order_source_t *order_source)
    : next(n) {
    cond_t interruptor;

    read_token_t read_token;
    next->store->new_read_token(&read_token);

    region_map_t<version_t> metainfo = next->store->get_metainfo(
        order_source->check_in("dummy_timestamper_t").with_read_mode(),
        &read_token,
        region_t::universe(),
        &interruptor);

    current_timestamp = state_timestamp_t::zero();
    metainfo.visit(metainfo.get_domain(),
        [&](const region_t &, const version_t &b) {
            state_timestamp_t region_ts = b.timestamp;
            current_timestamp = std::max(current_timestamp, region_ts);
        });
}

void dummy_timestamper_t::write(const write_t &_write,
                                write_response_t *response,
                                order_token_t otok) THROWS_NOTHING {
    order_sink.check_out(otok);
    current_timestamp = current_timestamp.next();
    next->write(_write, response, current_timestamp, otok);
}

void dummy_sharder_t::write(const write_t &_write,
                            write_response_t *response,
                            order_token_t tok,
                            const signal_t *interruptor) {
    if (interruptor->is_pulsed()) { throw interrupted_exc_t(); }

    the_shard.timestamper->write(_write, response, tok);
}

#if RDB_FDB_UNITTEST

dummy_namespace_interface_t::
dummy_namespace_interface_t(store_view_t *the_store, order_source_t
                            *order_source,
                            bool initialize_metadata)
{
    /* Initialize metadata everywhere */
    if (initialize_metadata) {
        cond_t interruptor;

        read_token_t read_token;
        the_store->new_read_token(&read_token);

        region_map_t<version_t> metainfo = the_store->get_metainfo(
            order_source->check_in("dummy_namespace_interface_t::"
                "dummy_namespace_interface_t (get_metainfo)").with_read_mode(),
            &read_token,
            region_t::universe(),
            &interruptor);

        rassert(metainfo.get_domain() == region_t::universe());
        metainfo.visit(region_t::universe(), [&](const region_t &, const version_t &b) {
            guarantee(b == version_t::zero());
        });

        write_token_t write_token;
        the_store->new_write_token(&write_token);

        the_store->set_metainfo(
                region_map_t<version_t>(
                    region_t::universe(), version_t::zero()),
                order_source->check_in("dummy_namespace_interface_t::"
                    "dummy_namespace_interface_t (set_metainfo)"),
                &write_token,
                write_durability_t::SOFT,
                &interruptor);
    }

    the_performer = make_scoped<dummy_performer_t>(the_store);
    the_timestamper = make_scoped<dummy_timestamper_t>(the_performer.get(), order_source);
    // Just one shard now.
    dummy_sharder_t::shard_t shards_of_this_db = dummy_sharder_t::shard_t(the_timestamper.get(), the_performer.get());

    sharder.init(new dummy_sharder_t(std::move(shards_of_this_db)));
}

#endif  // RDB_FDB_UNITTEST


}  // namespace unittest
