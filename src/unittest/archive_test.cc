// Copyright 2010-2012 RethinkDB, all rights reserved.
#include <string>

#include "unittest/gtest.hpp"

#include "containers/archive/boost_types.hpp"
#include "containers/archive/stl_types.hpp"

namespace unittest {

TEST(WriteMessageTest, Variant) {
    boost::variant<int32_t, std::string, int8_t> v("Hello, world!");

    write_message_t wm;

    serialize<cluster_version_t::LATEST_OVERALL>(&wm, v);

    std::string s = wm.send_to_string();

    ASSERT_EQ(2, s[0]);
    ASSERT_EQ(13, s[1]);  // The varint-encoded string length.
    ASSERT_EQ('H', s[2]);
    ASSERT_EQ('!', s[14]);
    ASSERT_EQ(15u, s.size());
}



}  // namespace unittest
