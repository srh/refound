// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "rdb_protocol/geo_traversal.hpp"

#include <cmath>

#include "rdb_protocol/batching.hpp"
#include "rdb_protocol/configured_limits.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/geo/distances.hpp"
#include "rdb_protocol/geo/exceptions.hpp"
#include "rdb_protocol/geo/geojson.hpp"
#include "rdb_protocol/geo/intersection.hpp"
#include "rdb_protocol/geo/lon_lat_types.hpp"
#include "rdb_protocol/geo/primitives.hpp"
#include "rdb_protocol/geo/s2/s2.h"
#include "rdb_protocol/geo/s2/s2latlng.h"
#include "rdb_protocol/profile.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/val.hpp"

using geo::S2Point;
using geo::S2LatLng;

// How many primary keys to keep in memory for avoiding processing the same
// document multiple times (efficiency optimization).
const size_t MAX_PROCESSED_SET_SIZE = 10000;

// How many grid cells to use for querying a secondary index.
// More cells result in better index traversal granularity, but also increase
// the CPU overhead for computing the grid covering.
const int QUERYING_GOAL_GRID_CELLS = 16;

// The radius used for the first batch of a get_nearest traversal.
// As a fraction of the equator's radius.
// The current value is equivalent to a radius of 10m on earth.
const double NEAREST_INITIAL_RADIUS_FRACTION = 10.0 / 6378137.0;

// By which factor to increase the radius between nearest batches (at maximum)
const double NEAREST_MAX_GROWTH_FACTOR = 2.0;

// The desired result size of each nearest batch
const double NEAREST_GOAL_BATCH_SIZE = 100.0;

// Number of vertices used in get_nearest traversal for approximating the
// current search range through a polygon.
const unsigned int NEAREST_NUM_VERTICES = 8;


geo_job_data_t::geo_job_data_t(
    ql::env_t *_env,
    ql::limit_read_last_key last_key,
    const ql::batchspec_t &batchspec,
    const std::vector<ql::transform_variant_t> &_transforms,
    const optional<ql::terminal_variant_t> &_terminal,
    is_stamp_read_t is_stamp_read)
    : env(_env),
      batcher(make_scoped<ql::batcher_t>(batchspec.to_batcher())),
      accumulator(_terminal.has_value()
                  ? ql::make_terminal(*_terminal)
                  : ql::make_append(std::move(last_key),
                                    // This causes the accumulator to include sindex_val
                                    // in the result, which we need for post-filtering in
                                    // reads for getIntersecting changefeeds.
                                    is_stamp_read == is_stamp_read_t::YES
                                        ? sorting_t::ASCENDING
                                        : sorting_t::UNORDERED,
                                    batcher.get(),
                                    require_sindexes_t::NO)) {
    for (size_t i = 0; i < _transforms.size(); ++i) {
        transformers.push_back(ql::make_op(_transforms[i]));
    }
    guarantee(transformers.size() == _transforms.size());
}

/* ----------- geo_intersecting_cb_t -----------*/
geo_intersecting_cb_t::geo_intersecting_cb_t(
        geo_sindex_data_t &&_sindex,
        ql::env_t *_env,
        std::set<std::pair<store_key_t, optional<uint64_t> > >
            *_distinct_emitted_in_out)
    : geo_index_traversal_helper_t(
        _env->interruptor),
      sindex(std::move(_sindex)),
      env(_env),
      distinct_emitted(_distinct_emitted_in_out) {
    guarantee(distinct_emitted != NULL);
    // We must disable profiler events for subtasks, because multiple instances
    // of `handle_pair`are going to run in parallel which  would otherwise corrupt
    // the sequence of events in the profiler trace.
    disabler.init(new profile::disabler_t(env->trace));
    sampler.init(new profile::sampler_t("Geospatial intersection traversal.",
                                        env->trace));
}

void geo_intersecting_cb_t::init_query(const ql::datum_t &_query_geometry) {
    query_geometry = _query_geometry;
    std::vector<geo::S2CellId> covering(
        compute_cell_covering(query_geometry, QUERYING_GOAL_GRID_CELLS));
    geo_index_traversal_helper_t::init_query(
        covering, compute_interior_cell_covering(query_geometry, covering));
}

// TODO: Would definitely be nice if we could be lazy about loading the value.
continue_bool_t geo_intersecting_cb_t::on_candidate(
        std::pair<const char *, size_t> key, std::pair<const char *, size_t> value,
        bool definitely_intersects_if_point)
        THROWS_ONLY(interrupted_exc_t) {
    guarantee(query_geometry.has());
    sampler->new_sample();

    store_key_t store_key(key.second, reinterpret_cast<const uint8_t *>(key.first));
    store_key_t primary_key(ql::datum_t::extract_primary(store_key));

    // Check if this document has already been processed (lower bound).
    optional<uint64_t> tag = ql::datum_t::extract_tag(store_key);
    std::pair<store_key_t, optional<uint64_t> > primary_and_tag(primary_key, tag);
    if (already_processed.count(primary_and_tag) > 0) {
        return continue_bool_t::CONTINUE;
    }
    // Check if this document has already been emitted.
    if (distinct_emitted->count(primary_and_tag) > 0) {
        return continue_bool_t::CONTINUE;
    }

    ql::datum_t val = datum_deserialize_from_vec(value.first, value.second);

    // TODO: Implement these perfmons
    // slice->stats.pm_keys_read.record();
    // slice->stats.pm_total_keys_read += 1;

    // Everything happens in key order after this. (Concurrent traversal used to
    // have a fifo token, allowing preceding code to run out of order.)
    // waiter.wait_interruptible();

    // row.get() or waiter.wait_interruptible() might have blocked, and another
    // coroutine could have found the document in the meantime. Re-check
    // distinct_emitted, so we don't emit the same document twice.
    if (distinct_emitted->count(primary_and_tag) > 0) {
        return continue_bool_t::CONTINUE;
    }

    try {
        // Post-filter the geometry based on an actual intersection test with
        // query_geometry.
        ql::env_t sindex_env(env->interruptor,
                             ql::return_empty_normal_batches_t::NO,
                             sindex.func_reql_version);
        ql::datum_t sindex_val =
            sindex.func->call(&sindex_env, val)->as_datum(&sindex_env);
        if (sindex.multi == sindex_multi_bool_t::MULTI
            && sindex_val.get_type() == ql::datum_t::R_ARRAY) {
            sindex_val = sindex_val.get(tag.get(), ql::NOTHROW);
            guarantee(sindex_val.has());
        }

        // Check if the index value is a point, so we can use
        // definitely_intersects_if_point
        bool definitely_intersects = false;
        if (definitely_intersects_if_point) {
            bool is_point = sindex_val.get_field("type").as_str() == "Point";
            if (is_point) {
                definitely_intersects = true;
            }
        }

        // TODO (daniel): This is a little inefficient because we re-parse
        // the query_geometry for each test.
        if ((definitely_intersects || geo_does_intersect(query_geometry, sindex_val))
            && post_filter(sindex_val, val)) {
            if (distinct_emitted->size() >= env->limits().array_size_limit()) {
                emit_error(ql::exc_t(ql::base_exc_t::RESOURCE,
                    "Array size limit exceeded during geospatial index traversal.",
                    ql::backtrace_id_t::empty()));
                return continue_bool_t::ABORT;
            }
            distinct_emitted->insert(primary_and_tag);
            return emit_result(std::move(sindex_val),
                               std::move(store_key),
                               std::move(val));
        } else {
            // Mark the document as processed so we don't have to load it again.
            // This is relevant only for polygons and lines, since those can be
            // encountered multiple times in the index.
            if (already_processed.size() < MAX_PROCESSED_SET_SIZE
                && sindex_val.get_field("type").as_str() != "Point") {
                already_processed.insert(primary_and_tag);
            }
            return continue_bool_t::CONTINUE;
        }
    } catch (const ql::exc_t &e) {
        emit_error(e);
        return continue_bool_t::ABORT;
    } catch (const geo_exception_t &e) {
        emit_error(ql::exc_t(ql::base_exc_t::LOGIC, e.what(),
                             ql::backtrace_id_t::empty()));
        return continue_bool_t::ABORT;
    } catch (const ql::base_exc_t &e) {
        emit_error(ql::exc_t(e, ql::backtrace_id_t::empty()));
        return continue_bool_t::ABORT;
    }
}


/* ----------- collect_all_geo_intersecting_cb_t -----------*/
collect_all_geo_intersecting_cb_t::collect_all_geo_intersecting_cb_t(
        geo_job_data_t &&_job,
        geo_sindex_data_t &&_sindex,
        const ql::datum_t &_query_geometry,
        rget_read_response_t *_resp_out)
    : geo_intersecting_cb_t(std::move(_sindex), _job.env, &distinct_emitted),
      job(std::move(_job)), response(_resp_out) {
    guarantee(response != NULL);
    init_query(_query_geometry);
}

void collect_all_geo_intersecting_cb_t::finish(
    continue_bool_t last_cb) THROWS_ONLY(interrupted_exc_t) {
    job.accumulator->finish(last_cb, &response->result);
}

bool collect_all_geo_intersecting_cb_t::post_filter(
        UNUSED const ql::datum_t &sindex_val,
        UNUSED const ql::datum_t &val)
        THROWS_ONLY(interrupted_exc_t, ql::base_exc_t, geo_exception_t) {
    return true;
}

continue_bool_t collect_all_geo_intersecting_cb_t::emit_result(
        ql::datum_t &&sindex_val,
        store_key_t &&key,
        ql::datum_t &&val)
        THROWS_ONLY(interrupted_exc_t, ql::base_exc_t, geo_exception_t) {
    ql::groups_t data;
    data = {{ql::datum_t(), ql::datums_t{std::move(val)}}};

    for (auto it = job.transformers.begin(); it != job.transformers.end(); ++it) {
        (*it)->apply_op(job.env, &data, [&]() { return sindex_val; });
    }
    return job.accumulator->apply_accumulator(job.env,
                              &data,
                              std::move(key),
                              [&]() { return sindex_val; });
}

void collect_all_geo_intersecting_cb_t::emit_error(
        const ql::exc_t &_error)
        THROWS_ONLY(interrupted_exc_t) {
    response->result = _error;
}


/* ----------- nearest traversal -----------*/
nearest_traversal_state_t::nearest_traversal_state_t(
        const lon_lat_point_t &_center,
        uint64_t _max_results,
        double _max_radius,
        const ellipsoid_spec_t &_reference_ellipsoid) :
    previous_size(0),
    processed_inradius(0.0),
    current_inradius(std::min(_max_radius,
            NEAREST_INITIAL_RADIUS_FRACTION * _reference_ellipsoid.equator_radius())),
    center(_center),
    max_results(_max_results),
    max_radius(_max_radius),
    reference_ellipsoid(_reference_ellipsoid) { }

continue_bool_t nearest_traversal_state_t::proceed_to_next_batch() {
    // Estimate the result density based on the previous batch
    const size_t previous_num_results = distinct_emitted.size() - previous_size;
    const double previous_area = M_PI *
        (current_inradius * current_inradius - processed_inradius * processed_inradius);
    const double previous_density =
        static_cast<double>(previous_num_results) / previous_area;

    processed_inradius = current_inradius;
    previous_size = distinct_emitted.size();

    // Adapt the radius based on the density of the previous batch.
    // Solve for current_inradius: NEAREST_GOAL_BATCH_SIZE =
    //   M_PI * (current_inradius^2 - processed_inradius^2) * previous_density
    // <=> current_inradius^2 =
    //   NEAREST_GOAL_BATCH_SIZE / M_PI / previous_density + processed_inradius^2
    if (previous_density != 0.0) {
        current_inradius = sqrt((NEAREST_GOAL_BATCH_SIZE / M_PI / previous_density)
                                + (processed_inradius * processed_inradius));
    } else {
        current_inradius = processed_inradius * NEAREST_MAX_GROWTH_FACTOR;
    }

    // In addition, put a limit on how fast the radius can grow, so we don't
    // increase it to ridiculous sizes just to encounter some cluster of results
    // later.
    current_inradius =
        std::min(current_inradius,
                 std::min(current_inradius * NEAREST_MAX_GROWTH_FACTOR, max_radius));

    if (processed_inradius >= max_radius || distinct_emitted.size() >= max_results) {
        return continue_bool_t::ABORT;
    } else {
        return continue_bool_t::CONTINUE;
    }
}

nearest_traversal_cb_t::nearest_traversal_cb_t(
        geo_sindex_data_t &&_sindex,
        ql::env_t *_env,
        nearest_traversal_state_t *_state) :
    geo_intersecting_cb_t(std::move(_sindex), _env, &_state->distinct_emitted),
    state(_state) {
    init_query_geometry();
}

void nearest_traversal_cb_t::init_query_geometry() {
    /*
    Note that S2 performs intersection tests on a sphere, while our distance
    metric is defined on an ellipsoid.

    The following must hold for this to work:

     A polygon constructed through build_polygon_with_exradius_at_most(center, r),
     must *not* intersect (using spherical geometry) with any point x that has
     a distance (on any given oblate ellipsoid) dist(center, x) > r.

     Similarly, a polygon constructed through
     build_polygon_with_inradius_at_least(center, r) must intersect (using
     spherical geometry) with *every* point x that has a distance (on any given
     ellipsoid) dist(center, x) <= r.

    There is a unit test in geo_primitives.cc to verify this numerically.
    */

    try {
        // 1. Construct a shape with an exradius of no more than state->processed_inradius.
        //    This is what we don't have to process again.
        std::vector<lon_lat_line_t> holes;
        if (state->processed_inradius > 0.0) {
            holes.push_back(build_polygon_with_exradius_at_most(
                state->center, state->processed_inradius,
                NEAREST_NUM_VERTICES, state->reference_ellipsoid));
        }

        // 2. Construct the outer shell, a shape with an inradius of at least
        //    state->current_inradius.
        lon_lat_line_t shell = build_polygon_with_inradius_at_least(
            state->center, state->current_inradius,
            NEAREST_NUM_VERTICES, state->reference_ellipsoid);

        ql::datum_t _query_geometry =
            construct_geo_polygon(shell, holes, ql::configured_limits_t::unlimited);
        init_query(_query_geometry);
    } catch (const geo_range_exception_t &e) {
        // The radius has become too large for constructing the query geometry.
        // Abort.
        throw geo_range_exception_t(strprintf(
            "The distance has become too large for continuing the indexed nearest "
            "traversal.  Consider specifying a smaller `max_dist` parameter.  "
            "(%s)", e.what()));
    } catch (const geo_exception_t &e) {
        crash("Geo exception thrown while initializing nearest query geometry: %s",
              e.what());
    }
}

bool nearest_traversal_cb_t::post_filter(
        const ql::datum_t &sindex_val,
        UNUSED const ql::datum_t &val)
        THROWS_ONLY(interrupted_exc_t, ql::base_exc_t, geo_exception_t) {

    // Filter out results that are outside of the current inradius
    const S2Point s2center =
        S2LatLng::FromDegrees(state->center.latitude, state->center.longitude).ToPoint();
    const double dist = geodesic_distance(s2center, sindex_val, state->reference_ellipsoid);
    return dist <= state->current_inradius;
}

continue_bool_t nearest_traversal_cb_t::emit_result(
        ql::datum_t &&sindex_val,
        UNUSED store_key_t &&key,
        ql::datum_t &&val)
        THROWS_ONLY(interrupted_exc_t, ql::base_exc_t, geo_exception_t) {
    // TODO (daniel): Could we avoid re-computing the distance? We have already
    //   done it in post_filter().
    const S2Point s2center =
        S2LatLng::FromDegrees(state->center.latitude, state->center.longitude).ToPoint();
    const double dist = geodesic_distance(s2center, sindex_val, state->reference_ellipsoid);
    result_acc.push_back(std::make_pair(dist, std::move(val)));

    return continue_bool_t::CONTINUE;
}

void nearest_traversal_cb_t::emit_error(
        const ql::exc_t &_error)
        THROWS_ONLY(interrupted_exc_t) {
    error.set(_error);
}

bool nearest_pairs_less(
        const std::pair<double, ql::datum_t> &p1,
        const std::pair<double, ql::datum_t> &p2) {
    // We only care about the distance, don't compare the actual data.
    return p1.first < p2.first;
}

void nearest_traversal_cb_t::finish(
        nearest_geo_read_response_t *resp_out) {
    guarantee(resp_out != NULL);
    if (error.has_value()) {
        resp_out->results_or_error = error.get();
    } else {
        std::sort(result_acc.begin(), result_acc.end(), &nearest_pairs_less);
        resp_out->results_or_error = std::move(result_acc);
    }
}
