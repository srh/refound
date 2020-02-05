#ifndef STORE_VIEW_HPP_
#define STORE_VIEW_HPP_

#include "btree/types.hpp"
#include "protocol_api.hpp"
#include "region/region_map.hpp"

class version_t;

#ifndef NDEBUG
// Checks that the metainfo has a certain value, or certain kind of value.
class metainfo_checker_t {
public:
    metainfo_checker_t(
            const region_t &r,
            const std::function<void(const region_t &, const version_t &)> &cb) :
        region(r), callback(cb) { }
    region_t region;
    std::function<void(const region_t &, const version_t &)> callback;
};

#endif  // NDEBUG

/* {read,write}_token_t hold the lock held when getting in line for the
   superblock. */
struct read_token_t {
    object_buffer_t<fifo_enforcer_sink_t::exit_read_t> main_read_token;
};

struct write_token_t {
    object_buffer_t<fifo_enforcer_sink_t::exit_write_t> main_write_token;
};

/* `store_view_t` is an abstract class that represents a region of a key-value store
for some protocol.  It covers some `region_t`, which is returned by `get_region()`.

In addition to the actual data, `store_view_t` is responsible for keeping track of
metadata which is keyed by region. The metadata is currently implemented as opaque
binary blob (`version_t`).
*/

class store_view_t : public home_thread_mixin_t {
public:
    virtual ~store_view_t() {
        home_thread_mixin_t::assert_thread();
    }

    virtual void note_reshard() = 0;

    virtual void new_read_token(read_token_t *token_out) = 0;
    virtual void new_write_token(write_token_t *token_out) = 0;

    /* Gets the metainfo. */
    virtual region_map_t<version_t> get_metainfo(
            order_token_t order_token,
            read_token_t *token,
            const region_t &region,
            signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t) = 0;

    /* Replaces the metainfo in `new_metainfo`'s domain with `new_metainfo`. */
    virtual void set_metainfo(
            const region_map_t<version_t> &new_metainfo,
            order_token_t order_token,
            write_token_t *token,
            write_durability_t durability,
            signal_t *interruptor) THROWS_ONLY(interrupted_exc_t) = 0;

    /* Performs a read. The read's region must be a subset of the store's region. */
    virtual void read(
            DEBUG_ONLY(const metainfo_checker_t& metainfo_expecter, )
            const read_t &read,
            read_response_t *response,
            read_token_t *token,
            signal_t *interruptor)
            THROWS_ONLY(interrupted_exc_t) = 0;

    /* Performs a write. `new_metainfo`'s region must be a subset of the store's region,
    and the write's region must be a subset of `new_metainfo`'s region. */
    virtual void write(
            DEBUG_ONLY(const metainfo_checker_t& metainfo_expecter, )
            const region_map_t<version_t> &new_metainfo,
            const write_t &write,
            write_response_t *response,
            write_durability_t durability,
            state_timestamp_t timestamp,
            order_token_t order_token,
            write_token_t *token,
            signal_t *interruptor)
            THROWS_ONLY(interrupted_exc_t) = 0;

    /* Deletes every key, and sets the metainfo for the db to
    version_t::zero(). */
    virtual void reset_data(
            write_durability_t durability,
            signal_t *interruptor)
            THROWS_ONLY(interrupted_exc_t) = 0;

protected:
    store_view_t() { }

private:
    DISABLE_COPYING(store_view_t);
};


#endif  // STORE_VIEW_HPP_
