#include "unittest/dummy_namespace_interface.hpp"

#include "unittest/clustering_utils.hpp"
#include "rdb_protocol/env.hpp"

namespace unittest {

#if RDB_FDB_UNITTEST

dummy_timestamper_t::dummy_timestamper_t(dummy_performer_t *n)
    : next(n) {
    cond_t interruptor;

    read_token_t read_token;
    next->store->new_read_token(&read_token);

    version_t metainfo = next->store->get_metainfo(
        &read_token,
        &interruptor);

    current_timestamp = metainfo.timestamp;
}


dummy_namespace_interface_t::
dummy_namespace_interface_t(store_view_t *the_store,
                            bool initialize_metadata)
{
    /* Initialize metadata everywhere */
    if (initialize_metadata) {
        cond_t interruptor;

        read_token_t read_token;
        the_store->new_read_token(&read_token);

        region_map_t<version_t> metainfo = the_store->get_metainfo(
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
                &write_token,
                write_durability_t::SOFT,
                &interruptor);
    }

    the_performer = make_scoped<dummy_performer_t>(the_store);
    the_timestamper = make_scoped<dummy_timestamper_t>(the_performer.get());
    // Just one shard now.
    dummy_sharder_t::shard_t shards_of_this_db = dummy_sharder_t::shard_t(the_timestamper.get(), the_performer.get());

    sharder.init(new dummy_sharder_t(std::move(shards_of_this_db)));
}

#endif  // RDB_FDB_UNITTEST


}  // namespace unittest
