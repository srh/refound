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


#endif  // STORE_VIEW_HPP_
