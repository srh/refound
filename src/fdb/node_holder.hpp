#ifndef RETHINKDB_FDB_NODE_HOLDER_HPP_
#define RETHINKDB_FDB_NODE_HOLDER_HPP_

#include <vector>

#include "concurrency/auto_drainer.hpp"
#include "concurrency/new_semaphore.hpp"
#include "concurrency/signal.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"
#include "fdb/id_types.hpp"
#include "fdb/reql_fdb.hpp"

struct fdb_job_info;

class fdb_node_holder : public home_thread_mixin_t {
public:
    explicit fdb_node_holder(FDBDatabase *fdb, const signal_t *interruptor,
        const proc_metadata_info &proc_metadata,
        const fdb_node_id &node_id,
        const fdb_cluster_id &expected_cluster_id);
    ~fdb_node_holder();

    // Tells the node holder to (try to) claim and start executing a job that has been
    // created and assigned to it (in a transaction that has already been committed).
    void supply_job(fdb_job_info job);

    // Shuts down the node holder coro.
    void shutdown(const signal_t *interruptor);

    fdb_node_id get_node_id() const { return node_id_; }

    template <class Callable>
    void update_proc_metadata(Callable&& callable) {
        bool mutated = callable(&proc_metadata_);
        if (mutated) {
            proc_metadata_cond_.pulse_if_not_already_pulsed();
        }
    }

private:
    void run_node_coro(auto_drainer_t::lock_t lock);

    FDBDatabase *fdb_;
    // TODO: Remove server_id_t entirely, or pass it in.
    const fdb_node_id node_id_;

    proc_metadata_info proc_metadata_;

    cond_t proc_metadata_cond_;

    // Supplied_jobs_ and supplied_job_sem_holder_ are 'protected' from concurrent
    // access by an "ASSERT_NO_CORO_WAITING" implicit mutex.
    std::vector<fdb_job_info> supplied_jobs_;

    // This gets blocked when there are no jobs.  Capacity is 1.
    new_semaphore_t supplied_job_sem_;

    // Holds the semaphore if supplied_jobs_ is empty.
    new_semaphore_in_line_t supplied_job_sem_holder_;

    bool initiated_shutdown_ = false;
    auto_drainer_t drainer_;
    DISABLE_COPYING(fdb_node_holder);
};



#endif  // RETHINKDB_FDB_NODE_HOLDER_HPP_
