#include "unittest/dummy_namespace_interface.hpp"

#include "unittest/clustering_utils.hpp"
#include "rdb_protocol/env.hpp"

namespace unittest {

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
