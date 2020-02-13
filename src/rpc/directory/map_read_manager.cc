// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rpc/directory/map_read_manager.tcc"

/* for unit tests */
template class directory_map_read_manager_t<int, int>;

#include "clustering/table_manager/table_metadata.hpp"
template class directory_map_read_manager_t<
    namespace_id_t, table_manager_bcard_t>;

#include "containers/empty_value.hpp"
template class directory_map_read_manager_t<
    server_id_t, empty_value_t>;
