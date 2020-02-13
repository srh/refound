// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_NAMESPACE_INTERFACE_REPOSITORY_HPP_
#define CLUSTERING_ADMINISTRATION_NAMESPACE_INTERFACE_REPOSITORY_HPP_

#include <map>

#include "clustering/administration/metadata.hpp"
#include "concurrency/auto_drainer.hpp"
#include "concurrency/one_per_thread.hpp"
#include "concurrency/promise.hpp"
#include "containers/clone_ptr.hpp"
#include "containers/incremental_lenses.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rpc/semilattice/view.hpp"

/* `namespace_repo_t` is a helper class for `real_reql_cluster_interface_t`. It's
responsible for constructing and caching `cluster_namespace_interface_t` objects. */

// NNN: Remove this header.

#endif /* CLUSTERING_ADMINISTRATION_NAMESPACE_INTERFACE_REPOSITORY_HPP_ */
