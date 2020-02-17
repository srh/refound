// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef RETHINKDB_WRITE_DURABILITY_HPP_
#define RETHINKDB_WRITE_DURABILITY_HPP_

#include <stdint.h>

#include "containers/archive/archive.hpp"

// write_durability_t::INVALID is an invalid value, notably it can't be serialized.
enum class write_durability_t { INVALID, SOFT, HARD };
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(write_durability_t, int8_t,
                                      write_durability_t::SOFT,
                                      write_durability_t::HARD);

#endif  // RETHINKDB_WRITE_DURABILITY_HPP_
