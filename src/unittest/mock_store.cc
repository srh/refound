#include "unittest/mock_store.hpp"

#include <deque>

#include "arch/timing.hpp"
#include "clustering/immediate_consistency/version.hpp"
#include "random.hpp"
#include "rdb_protocol/serialize_datum.hpp"
#include "rdb_protocol/store.hpp"

namespace unittest {

write_t mock_overwrite(std::string key, std::string value) {
    std::map<datum_string_t, ql::datum_t> m;
    m[datum_string_t("id")] = ql::datum_t(datum_string_t(key));
    m[datum_string_t("value")] = ql::datum_t(datum_string_t(value));

    point_write_t pw(
        store_key_t(key),
        ql::datum_t(std::move(m)));
    return write_t(pw, DURABILITY_REQUIREMENT_SOFT, profile_bool_t::DONT_PROFILE,
                   ql::configured_limits_t());
}

read_t mock_read(std::string key) {
    point_read_t pr((store_key_t(key)));
    return read_t(pr, profile_bool_t::DONT_PROFILE, read_mode_t::SINGLE);
}

std::string mock_parse_read_response(const read_response_t &rr) {
    const point_read_response_t *prr
        = boost::get<point_read_response_t>(&rr.response);
    guarantee(prr != nullptr);
    guarantee(prr->data.has());
    if (prr->data.get_type() == ql::datum_t::R_NULL) {
        // Behave like the old dummy_protocol_t.
        return "";
    }
    return prr->data.get_field("value").as_str().to_std();
}

#if RDB_FDB_UNITTEST
std::string mock_lookup(store_view_t *store, std::string key) {
#ifndef NDEBUG
    metainfo_checker_t checker(region_t::universe(),
        [](const region_t &, const version_t &) { });
#endif
    read_token_t token;
    store->new_read_token(&token);

    read_t r = mock_read(key);
    read_response_t rr;
    cond_t dummy_cond;
    store->read(DEBUG_ONLY(checker, )
                r,
                &rr,
                &token,
                &dummy_cond);
    return mock_parse_read_response(rr);
}
#endif  // RDB_FDB_UNITTEST

mock_store_t::mock_store_t(version_t universe_metainfo)
    : store_view_t(),
      metainfo_(universe_metainfo) { }
mock_store_t::~mock_store_t() { }

std::string mock_store_t::values(std::string key) {
    auto it = table_.find(store_key_t(key));
    if (it == table_.end()) {
        // Behave like the old dummy_protocol_t.
        return "";
    }
    return it->second.second.get_field("value").as_str().to_std();
}

repli_timestamp_t mock_store_t::timestamps(std::string key) {
    auto it = table_.find(store_key_t(key));
    if (it == table_.end()) {
        return repli_timestamp_t::distant_past;
    }
    return it->second.first;
}



}  // namespace unittest
