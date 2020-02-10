// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/distribution_progress.hpp"

#include "rdb_protocol/protocol.hpp"
#include "store_view.hpp"

distribution_progress_estimator_t::distribution_progress_estimator_t(
        store_view_t *,
        const signal_t *) {

    // TODO: distribution_counts is fake.
    distribution_counts.emplace(store_key_t::min(), 1);

    /* For the progress calculation we need partial sums for each key thus we
    calculate those from the results that the distribution query returns. */
    distribution_counts_sum = 0;
    for (auto &&distribution_count : distribution_counts) {
        distribution_count.second =
            (distribution_counts_sum += distribution_count.second);
    }
}

double distribution_progress_estimator_t::estimate_progress(
        const store_key_t &bound) const {
    if (distribution_counts_sum == 0) {
        return 0.0;
    }
    auto lower_bound = distribution_counts.lower_bound(bound);
    if (lower_bound != distribution_counts.end()) {
        return static_cast<double>(lower_bound->second) /
            static_cast<double>(distribution_counts_sum);
    } else {
        return 1.0;
    }
}

RDB_IMPL_SERIALIZABLE_2(distribution_progress_estimator_t,
    distribution_counts, distribution_counts_sum);
INSTANTIATE_SERIALIZABLE_FOR_CLUSTER(distribution_progress_estimator_t);
