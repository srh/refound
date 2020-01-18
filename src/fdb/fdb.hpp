#ifndef RETHINKDB_FDB_FDB_HPP_
#define RETHINKDB_FDB_FDB_HPP_

#define FDB_API_VERSION 620
#include "foundationdb/fdb_c.h"

/* This is the only file that includes foundationdb headers, because we need to set
FDB_API_VERSION before doing so. */

#endif  // RETHINKDB_FDB_FDB_HPP_
