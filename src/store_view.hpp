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

#endif  // STORE_VIEW_HPP_
