// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rpc/connectivity/cluster.hpp"

#ifndef _WIN32
#include <netinet/in.h>
#endif

#include <algorithm>
#include <functional>

#include "arch/io/network.hpp"
#include "arch/timing.hpp"
#include "clustering/administration/metadata.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/pmap.hpp"
#include "concurrency/semaphore.hpp"
#include "config/args.hpp"
#include "containers/archive/vector_stream.hpp"
#include "containers/archive/versioned.hpp"
#include "containers/object_buffer.hpp"
#include "containers/uuid.hpp"
#include "logger.hpp"
#include "rpc/semilattice/watchable.hpp"
#include "stl_utils.hpp"
#include "utils.hpp"

// Number of messages after which the message handling loop yields
#define MESSAGE_HANDLER_MAX_BATCH_SIZE           16

// The cluster communication protocol version.
static_assert(cluster_version_t::CLUSTER == cluster_version_t::v2_5_is_latest,
              "We need to update CLUSTER_VERSION_STRING when we add a new cluster "
              "version.");

// TODO: For RocksDB, make this distinct from non-RocksDB if necessary.
#define CLUSTER_VERSION_STRING "2.5.0"

const std::string connectivity_cluster_t::cluster_proto_header("RethinkDB cluster\n");
const std::string connectivity_cluster_t::cluster_version_string(CLUSTER_VERSION_STRING);

#if defined (__x86_64__) || defined (_WIN64) || defined (__s390x__) || defined(__arm64__) || defined(__aarch64__) || defined (__powerpc64__)
const std::string connectivity_cluster_t::cluster_arch_bitsize("64bit");
#elif defined (__i386__) || defined(__arm__) || defined(_WIN32)
const std::string connectivity_cluster_t::cluster_arch_bitsize("32bit");
#else
#error "Could not determine architecture"
#endif

#if defined (NDEBUG)
const std::string connectivity_cluster_t::cluster_build_mode("release");
#else
const std::string connectivity_cluster_t::cluster_build_mode("debug");
#endif

connectivity_cluster_t::connection_t::connection_t(
        connectivity_cluster_t *_parent,
        const peer_id_t &_peer_id,
        const server_id_t &_server_id,
        const peer_address_t &_peer_address) THROWS_NOTHING :
    peer_address(_peer_address),
    pm_collection(),
    pm_bytes_sent(secs_to_ticks(1), true, get_num_threads()),
    pm_collection_membership(
        &_parent->connectivity_collection,
        &pm_collection,
        uuid_to_str(_peer_id.get_uuid())),
    pm_bytes_sent_membership(&pm_collection, &pm_bytes_sent, "bytes_sent"),
    parent(_parent),
    peer_id(_peer_id),
    server_id(_server_id),
    drainers()
{
    pmap(get_num_threads(), [this](int thread_id) {
        on_thread_t thread_switcher((threadnum_t(thread_id)));
        parent->connections.get()->set_key_no_equals(
            peer_id,
            std::make_pair(this, auto_drainer_t::lock_t(drainers.get())));
    });
}

connectivity_cluster_t::connection_t::~connection_t() THROWS_NOTHING {
    // Drain out any users
    pmap(get_num_threads(), [this](int thread_id) {
        on_thread_t thread_switcher((threadnum_t(thread_id)));
        parent->connections.get()->delete_key(peer_id);
        drainers.get()->drain();
    });
}

// Helper function for the `run_t` constructor's initialization list
static peer_address_t our_peer_address() {
    // TODO: Ensure this is treated as a loopback peer address.
    return peer_address_t();
}

connectivity_cluster_t::run_t::run_t(
        connectivity_cluster_t *_parent,
        const server_id_t &_server_id) :
    /* The `connection_entry_t` constructor takes care of putting itself in the
    `connection_map` on each thread and notifying any listeners that we're now
    connected to ourself. The destructor will remove us from the
    `connection_map` and again notify any listeners. */
    connection_to_ourself(_parent, _parent->me, _server_id, our_peer_address())
{
}

connectivity_cluster_t::run_t::~run_t() {
    /* The member destructors take care of cutting off TCP connections, cleaning up, etc.
    */
}

class cluster_conn_closing_subscription_t : public signal_t::subscription_t {
public:
    explicit cluster_conn_closing_subscription_t(keepalive_tcp_conn_stream_t *conn) :
        conn_(conn) { }

    virtual void run() {
        if (conn_->is_read_open()) {
            conn_->shutdown_read();
        }
        if (conn_->is_write_open()) {
            conn_->shutdown_write();
        }
    }
private:
    keepalive_tcp_conn_stream_t *conn_;
    DISABLE_COPYING(cluster_conn_closing_subscription_t);
};


// Error-handling helper for connectivity_cluster_t::run_t::handle(). Returns true if
// handle() should return.
template <class T>
bool deserialize_and_check(cluster_version_t cluster_version,
                           keepalive_tcp_conn_stream_t *c, T *p, const char *peer) {
    archive_result_t res = deserialize_for_version(cluster_version, c, p);
    switch (res) {
    case archive_result_t::SUCCESS:
        return false;

        // Network error. Report nothing.
    case archive_result_t::SOCK_ERROR:
    case archive_result_t::SOCK_EOF:
        return true;

    case archive_result_t::RANGE_ERROR:
        logERR("could not deserialize data received from %s, closing connection", peer);
        return true;

    default:
        logERR("unknown error occurred on connection from %s, closing connection", peer);
        return true;
    }
}

template <class T>
bool deserialize_universal_and_check(keepalive_tcp_conn_stream_t *c,
                                     T *p, const char *peer) {
    archive_result_t res = deserialize_universal(c, p);
    switch (res) {
    case archive_result_t::SUCCESS:
        return false;

    case archive_result_t::SOCK_ERROR:
    case archive_result_t::SOCK_EOF:
        // Network error. Report nothing.
        return true;

    case archive_result_t::RANGE_ERROR:
        logERR("could not deserialize data received from %s, closing connection", peer);
        return true;

    default:
        logERR("unknown error occurred on connection from %s, closing connection", peer);
        return true;
    }
}

// TODO: Check if keepalive_tcp_conn_stream_t is used anymore.

// Reads a chunk of data off of the connection, buffer must have at least 'size' bytes
//  available to write into
bool read_header_chunk(keepalive_tcp_conn_stream_t *conn, char *buffer, int64_t size,
        const char *peer) {
    int64_t r = conn->read(buffer, size);
    if (-1 == r) {
        logWRN("Network error while receiving clustering header from %s, closing connection.", peer);
        return false; // network error.
    }
    rassert(r >= 0);
    if (0 == r) {
        logWRN("Received incomplete clustering header from %s, closing connection.", peer);
        return false;
    }
    return true;
}

// Reads a uint64_t for size, then the string data
bool deserialize_compatible_string(keepalive_tcp_conn_stream_t *conn,
                                   std::string *str_out,
                                   const char *peer) {
    uint64_t raw_size;
    archive_result_t res = deserialize_universal(conn, &raw_size);
    if (res != archive_result_t::SUCCESS) {
        logWRN("Network error while receiving clustering header from %s, closing connection", peer);
        return false;
    }

    if (raw_size > 4096) {
        logWRN("Received excessive string size in header from peer %s, closing connection", peer);
        return false;
    }

    size_t size = raw_size;
    scoped_array_t<char> buffer(size);
    if (!read_header_chunk(conn, buffer.data(), size, peer)) {
        return false;
    }

    str_out->assign(buffer.data(), size);
    return true;
}

// You must update deserialize_universal(read_stream_t *, handshake_result_t *)
// below when changing this enum.
enum class handshake_result_code_t {
    SUCCESS = 0,
    UNRECOGNIZED_VERSION = 1,
    INCOMPATIBLE_ARCH = 2,
    INCOMPATIBLE_BUILD = 3,
    PASSWORD_MISMATCH = 4,
    UNKNOWN_ERROR = 5,
    UNEXPECTED_SERVER_ID = 6
};

class handshake_result_t {
public:
    handshake_result_t() { }
    static handshake_result_t success() {
        return handshake_result_t(handshake_result_code_t::SUCCESS);
    }
    static handshake_result_t error(handshake_result_code_t error_code,
                                    const std::string &additional_info) {
        return handshake_result_t(error_code, additional_info);
    }

    handshake_result_code_t get_code() const {
        return code;
    }

    std::string get_error_reason() const {
        if (code == handshake_result_code_t::UNKNOWN_ERROR) {
            return error_code_string + " (" + additional_info + ")";
        } else {
            return get_code_as_string() + " (" + additional_info + ")";
        }
    }

private:
    std::string get_code_as_string() const {
        switch (code) {
            case handshake_result_code_t::SUCCESS:
                return "success";
            case handshake_result_code_t::UNRECOGNIZED_VERSION:
                return "unrecognized or incompatible version";
            case handshake_result_code_t::INCOMPATIBLE_ARCH:
                return "incompatible architecture";
            case handshake_result_code_t::INCOMPATIBLE_BUILD:
                return "incompatible build mode";
            case handshake_result_code_t::PASSWORD_MISMATCH:
                return "no admin password";
            case handshake_result_code_t::UNEXPECTED_SERVER_ID:
                return "unexpected server id";
            case handshake_result_code_t::UNKNOWN_ERROR:
                unreachable();
            default:
                unreachable();
        }
    }

    handshake_result_t(handshake_result_code_t _error_code,
                       const std::string &_additional_info)
        : code(_error_code), additional_info(_additional_info) {
        guarantee(code != handshake_result_code_t::UNKNOWN_ERROR);
        guarantee(code != handshake_result_code_t::SUCCESS);
        error_code_string = get_code_as_string();
    }
    explicit handshake_result_t(handshake_result_code_t _success)
        : code(_success) {
        guarantee(code == handshake_result_code_t::SUCCESS);
    }

    friend void serialize_universal(write_message_t *, const handshake_result_t &);
    friend archive_result_t deserialize_universal(read_stream_t *, handshake_result_t *);

    handshake_result_code_t code;
    // In case code is UNKNOWN_ERROR, this error message
    // will contain a human-readable description of the error code.
    // The idea is that if we are talking to a newer node on the other side,
    // it might send us some error codes that we don't understand. However the
    // other node will know how to format that error into an error message.
    std::string error_code_string;
    std::string additional_info;
};

// It is ok to add new result codes to handshake_result_code_t.
// However the existing code and the structure of handshake_result_t must be
// kept compatible.
void serialize_universal(write_message_t *wm, const handshake_result_t &r) {
    guarantee(r.code != handshake_result_code_t::UNKNOWN_ERROR,
              "Cannot serialize an unknown handshake result code");
    serialize_universal(wm, static_cast<uint8_t>(r.code));
    serialize_universal(wm, r.error_code_string);
    serialize_universal(wm, r.additional_info);
}
archive_result_t deserialize_universal(read_stream_t *s, handshake_result_t *out) {
    archive_result_t res;
    uint8_t code_int;
    res = deserialize_universal(s, &code_int);
    if (res != archive_result_t::SUCCESS) {
        return res;
    }
    if (code_int >= static_cast<uint8_t>(handshake_result_code_t::UNKNOWN_ERROR)) {
        // Unrecognized error code. Fall back to UNKNOWN_ERROR.
        out->code = handshake_result_code_t::UNKNOWN_ERROR;
    } else {
        out->code = static_cast<handshake_result_code_t>(code_int);
    }
    res = deserialize_universal(s, &out->error_code_string);
    if (res != archive_result_t::SUCCESS) {
        return res;
    }
    res = deserialize_universal(s, &out->additional_info);
    return res;
}

void fail_handshake(keepalive_tcp_conn_stream_t *conn,
                    const char *peername,
                    const handshake_result_t &reason,
                    bool send_error_to_peer = true) {
    logWRN("Connection attempt from %s failed, reason: %s ",
           peername, sanitize_for_logger(reason.get_error_reason()).c_str());

    if (send_error_to_peer) {
        // Send the reason for the failed handshake to the other side, so it can
        // print a nice message or do something else with it.
        write_message_t wm;
        serialize_universal(&wm, reason);
        if (send_write_message(conn, &wm)) {
            // network error. Ignore
        }
    }
}

connectivity_cluster_t::connectivity_cluster_t() THROWS_NOTHING :
    me(peer_id_t(generate_uuid())),
    /* We assign threads from the highest thread number downwards. This is to reduce the
    potential for conflicting with btree threads, which assign threads from the lowest
    thread number upwards. */
    thread_allocator([](threadnum_t a, threadnum_t b) {
        return a.threadnum > b.threadnum;
    }),
    connectivity_collection(),
    stats_membership(&get_global_perfmon_collection(), &connectivity_collection, "connectivity")
{
    for (int i = 0; i < max_message_tag; i++) {
        message_handlers[i] = nullptr;
    }
}

connectivity_cluster_t::~connectivity_cluster_t() THROWS_NOTHING {
#ifdef ENABLE_MESSAGE_PROFILER
    std::map<std::string, std::pair<uint64_t, uint64_t> > total_counts;
    pmap(get_num_threads(), [&](int num) {
        std::map<std::string, std::pair<uint64_t, uint64_t> > copy;
        {
            on_thread_t thread_switcher((threadnum_t(num)));
            copy = *message_profiler_counts.get();
        }
        for (const auto &pair : copy) {
            total_counts[pair.first].first += pair.second.first;
            total_counts[pair.first].second += pair.second.second;
        }
    });

    std::string output_filename =
        strprintf("message_profiler_out_%d.txt", static_cast<int>(getpid()));
    FILE *file = fopen(output_filename.c_str(), "w");
    guarantee(file != nullptr, "Cannot open %s for writing", output_filename.c_str());
    for (const auto &pair : total_counts) {
        fprintf(file, "%" PRIu64 " %" PRIu64 " %s\n",
            pair.second.first, pair.second.second, pair.first.c_str());
    }
    fclose(file);
#endif
}

peer_id_t connectivity_cluster_t::get_me() THROWS_NOTHING {
    return me;
}

watchable_map_t<peer_id_t, connectivity_cluster_t::connection_pair_t> *
connectivity_cluster_t::get_connections() THROWS_NOTHING {
    return connections.get();
}

connectivity_cluster_t::connection_t *connectivity_cluster_t::get_connection(
        peer_id_t peer_id, auto_drainer_t::lock_t *keepalive_out) THROWS_NOTHING {
    connectivity_cluster_t::connection_t *conn;
    connections.get()->read_key(peer_id,
        [&](const connection_pair_t *value) {
            if (value == nullptr) {
                conn = nullptr;
            } else {
                conn = value->first;
                *keepalive_out = value->second;
            }
        });
    return conn;
}

void connectivity_cluster_t::send_message(connection_t *connection,
                                     auto_drainer_t::lock_t connection_keepalive,
                                     message_tag_t tag,
                                     cluster_send_message_write_callback_t *callback) {
    // We could be on _any_ thread.

    /* If the connection is being closed, just drop the message now. It's not going
    to actually get sent anyway. That way we avoid getting in line for the send_mutex. */
    if (connection_keepalive.get_drain_signal()->is_pulsed()) {
        return;
    }

    /* We currently write the message to a vector_stream_t, then
       serialize that as a string. It's horribly inefficient, of course. */
    // TODO: If we don't do it this way, we (or the caller) will need
    // to worry about having the writer run on the connection thread.
    vector_stream_t buffer;
    // Reserve some space to reduce overhead (especially for small messages)
    buffer.reserve(1024);
    {
        ASSERT_FINITE_CORO_WAITING;
        callback->write(&buffer);
    }

#ifndef NDEBUG
    connection_keepalive.assert_is_holding(connection->drainers.get());

    /* We're allowed to block indefinitely, but it's tempting to write code on
    the assumption that we won't. This might catch some programming errors. */
    if (randint(10) == 0) {
        cond_t non_interruptor;
        nap(10, &non_interruptor);
    }
#endif

    size_t bytes_sent = buffer.vector().size();

#ifdef ENABLE_MESSAGE_PROFILER
    std::pair<uint64_t, uint64_t> *stats =
        &(*message_profiler_counts.get())[callback->message_profiler_tag()];
    stats->first += 1;
    stats->second += bytes_sent;
#endif

    if (connection->is_loopback()) {
        // We could be on any thread here! Oh no!
        rassert(message_handlers[tag], "No message handler for tag %" PRIu8, tag);
        message_handlers[tag]->on_local_message(connection, connection_keepalive,
            std::move(buffer.vector()));
    } else {
        crash("Connection is always loopback.");
    }

    connection->pm_bytes_sent.record(bytes_sent);
}

cluster_message_handler_t::cluster_message_handler_t(
        connectivity_cluster_t *cm,
        connectivity_cluster_t::message_tag_t t) :
    connectivity_cluster(cm), tag(t)
{
    rassert(tag != connectivity_cluster_t::heartbeat_tag,
        "Tag %" PRIu8 " is reserved for heartbeat messages.",
        connectivity_cluster_t::heartbeat_tag);
    rassert(connectivity_cluster->message_handlers[tag] == nullptr);
    connectivity_cluster->message_handlers[tag] = this;
}

cluster_message_handler_t::~cluster_message_handler_t() {
    rassert(connectivity_cluster->message_handlers[tag] == this);
    connectivity_cluster->message_handlers[tag] = nullptr;
}

