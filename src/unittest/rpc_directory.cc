// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "arch/timing.hpp"
#include "clustering/administration/metadata.hpp"
#include "rpc/connectivity/cluster.hpp"
#include "rpc/directory/map_read_manager.hpp"
#include "rpc/directory/map_write_manager.hpp"
#include "rpc/directory/read_manager.hpp"
#include "rpc/directory/write_manager.hpp"
#include "unittest/gtest.hpp"
#include "unittest/clustering_utils.hpp"
#include "unittest/dummy_metadata_controller.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

/* `OneNode` starts a single directory node, then shuts it down again. */
TPTEST(RPCDirectoryTest, OneNode) {
    connectivity_cluster_t c;
    directory_read_manager_t<int> read_manager(&c, 'D');
    watchable_variable_t<int> watchable(5);
    directory_write_manager_t<int> write_manager(&c, 'D', watchable.get_watchable());
    test_cluster_run_t cr(&c);
    let_stuff_happen();
}

/* `DestructorRace` tests a nasty race condition that we had at some point. */
TPTEST(RPCDirectoryTest, DestructorRace) {
    connectivity_cluster_t c;
    directory_read_manager_t<int> rm(&c, 'D');
    watchable_variable_t<int> w(5);
    directory_write_manager_t<int> wm(&c, 'D', w.get_watchable());
    test_cluster_run_t cr(&c);

    w.set_value(6);
}

}   /* namespace unittest */
