// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_ISSUES_NAME_COLLISION_HPP_
#define CLUSTERING_ADMINISTRATION_ISSUES_NAME_COLLISION_HPP_

#include <vector>
#include <set>
#include <string>

#include "clustering/administration/issues/issue.hpp"
#include "rpc/connectivity/server_id.hpp"
#include "rpc/semilattice/view.hpp"

class name_collision_issue_tracker_t : public issue_tracker_t {
public:
    explicit name_collision_issue_tracker_t(
        server_config_client_t *_server_config_client);

    ~name_collision_issue_tracker_t();

    std::vector<scoped_ptr_t<issue_t> > get_issues(const signal_t *interruptor) const;

private:
    server_config_client_t *server_config_client;

    DISABLE_COPYING(name_collision_issue_tracker_t);
};

#endif /* CLUSTERING_ADMINISTRATION_ISSUES_NAME_COLLISION_HPP_ */
