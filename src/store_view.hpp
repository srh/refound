#ifndef STORE_VIEW_HPP_
#define STORE_VIEW_HPP_

#include "btree/types.hpp"
#include "protocol_api.hpp"

class version_t;

#ifndef NDEBUG
// Checks that the metainfo has a certain value, or certain kind of value.
class metainfo_checker_t {
public:
    metainfo_checker_t(
            const std::function<void(const version_t &)> &cb) :
        callback(cb) { }
    std::function<void(const version_t &)> callback;
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

protected:
    store_view_t() { }

private:
    DISABLE_COPYING(store_view_t);
};


#endif  // STORE_VIEW_HPP_
