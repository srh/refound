#ifndef REGION_REGION_HPP_
#define REGION_REGION_HPP_

#include <vector>

#include "btree/keys.hpp"

typedef key_range_t region_t;

enum region_join_result_t { REGION_JOIN_OK, REGION_JOIN_BAD_JOIN, REGION_JOIN_BAD_REGION };

inline bool region_is_empty(const key_range_t &r) {
    return r.is_empty();
}

inline bool region_is_superset(const key_range_t &potential_superset, const key_range_t &potential_subset) THROWS_NOTHING {
    return potential_superset.is_superset(potential_subset);
}

inline key_range_t region_intersection(const key_range_t &r1, const key_range_t &r2) THROWS_NOTHING {
    return r1.intersection(r2);
}

inline bool region_overlaps(const key_range_t &r1, const key_range_t &r2) THROWS_NOTHING {
    return r1.overlaps(r2);
}

inline bool region_contains_key(const key_range_t &kr, const store_key_t &k) {
    return kr.contains_key(k);
}


MUST_USE region_join_result_t region_join(const std::vector<key_range_t> &vec, key_range_t *out) THROWS_NOTHING;

#endif  // REGION_REGION_HPP_
