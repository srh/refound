#ifndef UNITTEST_MOCK_STORE_HPP_
#define UNITTEST_MOCK_STORE_HPP_

#include <map>
#include <utility>
#include <string>

#include "rdb_protocol/protocol.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

write_t mock_overwrite(std::string key, std::string value);

read_t mock_read(std::string key);

std::string mock_parse_read_response(const read_response_t &rr);

}  // namespace unittest

#endif  // UNITTEST_MOCK_STORE_HPP_
