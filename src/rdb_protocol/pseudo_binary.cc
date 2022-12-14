// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/pseudo_binary.hpp"

#include "errors.hpp"

#include "utils.hpp"
#include "rapidjson/rapidjson.h"
#include "rdb_protocol/base64.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/error.hpp"

#include "debug.hpp"

namespace ql {
namespace pseudo {

const char *const binary_string = "BINARY";
const char *const data_key = "data";

// Given a raw data string, encodes it into a `r.binary` pseudotype with base64 encoding
void encode_base64_ptype(
        const datum_string_t &data,
        rapidjson::Writer<rapidjson::StringBuffer> *writer) {
    writer->StartObject();
    writer->Key(datum_t::reql_type_string.data(), datum_t::reql_type_string.size());
    writer->String(binary_string);
    const std::string encoded_data = encode_base64(data.data(), data.size());
    writer->Key(data_key);
    writer->String(encoded_data.data(), encoded_data.size());
    writer->EndObject();
}

rapidjson::Value encode_base64_ptype(const datum_string_t &data,
                                     rapidjson::Value::AllocatorType *allocator) {
    rapidjson::Value res(rapidjson::kObjectType);
    res.AddMember(rapidjson::Value(datum_t::reql_type_string.data(),
                                   datum_t::reql_type_string.size(),
                                   *allocator),
                  rapidjson::Value(binary_string, *allocator), *allocator);
    res.AddMember(rapidjson::Value(data_key, *allocator),
                  rapidjson::Value(encode_base64(data.data(), data.size()).c_str(),
                                   *allocator), *allocator);
    return res;
}

// Given a `r.binary` pseudotype with base64 encoding, decodes it into a raw data string
datum_string_t decode_base64_ptype(
        const std::vector<std::pair<datum_string_t, datum_t> > &ptype) {
    bool has_data = false;
    datum_string_t res;
    for (auto it = ptype.begin(); it != ptype.end(); ++it) {
        if (it->first == datum_t::reql_type_string) {
            r_sanity_check(it->second.as_str() == binary_string);
        } else if(it->first == data_key) {
            has_data = true;
            datum_string_t base64_data = it->second.as_str();
            std::string decoded_str = decode_base64(base64_data.data(),
                                                    base64_data.size());
            res = datum_string_t(decoded_str.size(), decoded_str.data());
        } else {
            rfail_datum(base_exc_t::LOGIC,
                        "Invalid binary pseudotype: illegal `%s` key.",
                        it->first.to_std().c_str());
        }
    }
    rcheck_datum(has_data, base_exc_t::LOGIC,
                 strprintf("Invalid binary pseudotype: lacking `%s` key.",
                           data_key).c_str());
    return res;
}

} // namespace pseudo
} // namespace ql
