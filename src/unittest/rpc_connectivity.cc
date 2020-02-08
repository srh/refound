// Copyright 2010-2014 RethinkDB, all rights reserved.
#include <functional>

#ifdef _WIN32
#include "windows.hpp"
#include <ws2tcpip.h> // NOLINT
#include <iphlpapi.h> // NOLINT
#endif

#include "arch/runtime/thread_pool.hpp"
#include "arch/timing.hpp"
#include "containers/scoped.hpp"
#include "containers/archive/socket_stream.hpp"
#include "unittest/clustering_utils.hpp"
#include "unittest/unittest_utils.hpp"
#include "rpc/connectivity/cluster.hpp"
#include "unittest/gtest.hpp"

namespace unittest {

/* `recording_test_application_t` sends and receives integers over a
`message_service_t`. It keeps track of the integers it has received.
*/

class recording_test_application_t :
    public home_thread_mixin_t,
    public cluster_message_handler_t
{
public:
    explicit recording_test_application_t(connectivity_cluster_t *cm,
                                          connectivity_cluster_t::message_tag_t _tag) :
        cluster_message_handler_t(cm, _tag),
        sequence_number(0)
        { }
    void send(int message, peer_id_t peer) {
        auto_drainer_t::lock_t connection_keepalive;
        connectivity_cluster_t::connection_t *connection =
            get_connectivity_cluster()->get_connection(peer, &connection_keepalive);
        if (connection) {
            send(message, connection, connection_keepalive);
        }
    }
    void send(int message, connectivity_cluster_t::connection_t *connection,
            auto_drainer_t::lock_t connection_keepalive) {
        class writer_t : public cluster_send_message_write_callback_t {
        public:
            explicit writer_t(int _data) : data(_data) { }
            virtual ~writer_t() { }
            void write(write_stream_t *stream) {
                write_message_t wm;
                serialize<cluster_version_t::CLUSTER>(&wm, data);
                int res = send_write_message(stream, &wm);
                if (res) { throw fake_archive_exc_t(); }
            }
#ifdef ENABLE_MESSAGE_PROFILER
            const char *message_profiler_tag() const {
                return "unittest";
            }
#endif
            int32_t data;
        } writer(message);
        get_connectivity_cluster()->send_message(connection, connection_keepalive,
            get_message_tag(), &writer);
    }
    void expect(int message, peer_id_t peer) {
        expect_delivered(message);
        assert_thread();
        EXPECT_TRUE(inbox[message] == peer);
    }
    void expect_delivered(int message) {
        assert_thread();
        EXPECT_TRUE(inbox.find(message) != inbox.end());
    }
    void expect_undelivered(int message) {
        assert_thread();
        EXPECT_TRUE(inbox.find(message) == inbox.end());
    }
    void expect_order(int first, int second) {
        expect_delivered(first);
        expect_delivered(second);
        assert_thread();
        EXPECT_LT(timing[first], timing[second]);
    }

private:
    void on_message(connectivity_cluster_t::connection_t *connection,
                    auto_drainer_t::lock_t,
                    read_stream_t *stream) {
        int i;
        archive_result_t res
            = deserialize<cluster_version_t::CLUSTER>(stream, &i);
        if (bad(res)) { throw fake_archive_exc_t(); }
        on_thread_t th(home_thread());
        inbox[i] = connection->get_peer_id();
        timing[i] = sequence_number++;
    }

    std::map<int, peer_id_t> inbox;
    std::map<int, int> timing;
    int sequence_number;
};


/* `PeerIDSemantics` makes sure that `peer_id_t::is_nil()` works as expected. */
TPTEST_MULTITHREAD(RPCConnectivityTest, PeerIDSemantics, 3) {
    peer_id_t nil_peer;
    ASSERT_TRUE(nil_peer.is_nil());

    connectivity_cluster_t cluster_node;
    ASSERT_FALSE(cluster_node.get_me().is_nil());
}

class meanwhile_t {
public:
    meanwhile_t() DEBUG_ONLY(: started(false)) { }

    template <class callable_t>
    explicit meanwhile_t(callable_t&& run) DEBUG_ONLY(: started(false)) {
        start(std::forward<callable_t>(run));
    }

    template <class callable_t>
    void start(callable_t run) {
#ifndef NDEBUG
        rassert(!started);
        started = true;
#endif
        run_ = run;
        coro_t::spawn_sometime([this](){
            run_(&interrupt);
            done.pulse();
        });
    }

    ~meanwhile_t() {
        if (!done.is_pulsed()) {
            interrupt.pulse();
            done.wait_lazily_ordered();
        }
    }

private:
#ifndef NDEBUG
    bool started;
#endif
    cond_t interrupt;
    cond_t done;
    std::function<void(signal_t*)> run_;
};

class on_timeout_t {
public:
    template <class callable_t>
    on_timeout_t(int64_t ms, callable_t handler) {
        handler_ = handler;
        timer.start(ms);
        waiter.start([this](const signal_t *interruptor) {
            wait_any_t both(&timer, interruptor);
            both.wait();
            if (timer.is_pulsed()) {
                handler_();
            }
        });
    }

    bool timed_out() {
        return timer.is_pulsed();
    }

private:
    std::function<void()> handler_;
    signal_timer_t timer;
    meanwhile_t waiter;
};

// Make sure each side of the connection is closed
void check_tcp_closed(tcp_conn_stream_t *stream) {

    // Allow 6 seconds before timing out
    on_timeout_t timeout(6000, [stream](){
        stream->shutdown_read();
        stream->shutdown_write();
    });

    char buffer[1024];
    int64_t res;
    do {
        res = stream->read(&buffer, 1024);
    } while (res > 0);

    do {
        let_stuff_happen();
        res = stream->write("a", 1);
    } while(res != -1);

    if (timeout.timed_out()) {
        FAIL() << "timed out";
    }

    ASSERT_FALSE(stream->is_write_open());
    ASSERT_FALSE(stream->is_read_open());
}

// TODO: Is this unused?
std::set<host_and_port_t> convert_from_any_port(const std::set<host_and_port_t> &addresses,
                                                port_t actual_port) {
    std::set<host_and_port_t> result;
    for (auto it = addresses.begin(); it != addresses.end(); ++it) {
        if (it->port().value() == 0) {
            result.insert(host_and_port_t(it->host(), actual_port));
        } else {
            result.insert(*it);
        }
    }
    return result;
}

}   /* namespace unittest */
