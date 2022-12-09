// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "clustering/main/command_line.hpp"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef ENABLE_TLS
#include <openssl/ssl.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/opensslv.h>
#endif

#ifndef _WIN32
#include <pwd.h>
#include <grp.h>
#endif

#ifdef _WIN32
#include "errors.hpp"
#include <Windows.h>
#include <Shellapi.h>
#endif

// Needed for determining rethinkdb binary path below
#if defined(__MACH__)
#include <mach-o/dyld.h>
#elif defined(__FreeBSD_version)
#include <sys/sysctl.h>
#endif

#include <functional>
#include <limits>

#include <re2/re2.h>

#include "arch/io/disk.hpp"
#include "arch/io/openssl.hpp"
#include "arch/os_signal.hpp"
#include "arch/runtime/starter.hpp"
#include "arch/filesystem.hpp"

#include "extproc/extproc_spawner.hpp"
#include "clustering/auth/user.hpp"
#include "clustering/auth/username.hpp"
#include "clustering/auth/password.hpp"
#include "clustering/main/cache_size.hpp"
#include "clustering/main/names.hpp"
#include "clustering/main/options.hpp"
#include "clustering/main/ports.hpp"
#include "clustering/main/serve.hpp"
#include "clustering/main/directory_lock.hpp"
#include "clustering/main/windows_service.hpp"
#include "clustering/logs/log_writer.hpp"
#include "clustering/main/path.hpp"
#include "containers/scoped.hpp"
#include "crypto/random.hpp"
#include "logger.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/reql_fdb_utils.hpp"
#include "fdb/startup_shutdown.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

#define RETHINKDB_EXPORT_SCRIPT "rethinkdb-export"
#define RETHINKDB_IMPORT_SCRIPT "rethinkdb-import"
#define RETHINKDB_DUMP_SCRIPT "rethinkdb-dump"
#define RETHINKDB_RESTORE_SCRIPT "rethinkdb-restore"
#define RETHINKDB_INDEX_REBUILD_SCRIPT "rethinkdb-index-rebuild"
#define RETHINKDB_REPL_SCRIPT "rethinkdb-repl"

using options::obsolescence;

namespace cluster_defaults {
const int reconnect_timeout = (24 * 60 * 60);    // 24 hours (in secs)
}  // namespace cluster_defaults

constexpr const char *CLUSTER_ID_FILENAME = "cluster_id";
constexpr const char *NODE_ID_FILENAME = "node_id";
constexpr const char *LOG_FILE_DEFAULT_FILENAME = "log_file";
constexpr const char *FDB_CLUSTER_DEFAULT_FILENAME = "fdb.cluster";

MUST_USE bool numwrite(const char *path, int number) {
    // Try to figure out what this function does.
    FILE *fp1 = fopen(path, "w");
    if (fp1 == nullptr) {
        return false;
    }
    fprintf(fp1, "%d", number);
    fclose(fp1);
    return true;
}

static std::string pid_file;

void remove_pid_file() {
    if (!pid_file.empty()) {
        remove(pid_file.c_str());
        pid_file.clear();
    }
}

int check_pid_file(const std::string &pid_filepath) {
    guarantee(pid_filepath.size() > 0);

    if (access(pid_filepath.c_str(), F_OK) == 0) {
        logERR("The pid-file specified already exists. This might mean that an instance is already running.");
        return EXIT_FAILURE;
    }

    if (access(strdirname(pid_filepath).c_str(), W_OK) == -1) {
        logERR("Cannot access the pid-file directory.");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int write_pid_file(const std::string &pid_filepath) {
    guarantee(pid_filepath.size() > 0);

    // Be very careful about modifying this. It is important that the code that removes the
    // pid-file only run if the checks here pass. Right now, this is guaranteed by the return on
    // failure here.
    if (!pid_file.empty()) {
        logERR("Attempting to write pid-file twice.");
        return EXIT_FAILURE;
    }

    if (check_pid_file(pid_filepath) == EXIT_FAILURE) {
        return EXIT_FAILURE;
    }

    if (!numwrite(pid_filepath.c_str(), getpid())) {
        logERR("Writing to the specified pid-file failed.");
        return EXIT_FAILURE;
    }
    pid_file = pid_filepath;
    atexit(remove_pid_file);

    return EXIT_SUCCESS;
}

// Extracts an option that appears either zero or once.  Multiple appearances are not allowed (or
// expected).
optional<std::string> get_optional_option(const std::map<std::string, options::values_t> &opts,
                                                 const std::string &name,
                                                 std::string *source_out) {
    auto it = opts.find(name);
    if (it == opts.end()) {
        *source_out = "nowhere";
        return optional<std::string>();
    }

    if (it->second.values.empty()) {
        *source_out = it->second.source;
        return optional<std::string>();
    }

    if (it->second.values.size() == 1) {
        *source_out = it->second.source;
        return make_optional(it->second.values[0]);
    }

    throw std::logic_error("Option '%s' appears multiple times (when it should only appear once.)");
}

optional<std::string> get_optional_option(const std::map<std::string, options::values_t> &opts,
                                                 const std::string &name) {
    std::string source;
    return get_optional_option(opts, name, &source);
}

#ifndef _WIN32
// Returns false if the group was not found.  This function replaces a call to
// getgrnam(3).  That's right, getgrnam_r's interface is such that you have to
// go through these shenanigans.
bool get_group_id(const char *name, gid_t *group_id_out) {
    // On Linux we can use sysconf to learn what the bufsize should be but on OS
    // X we just have to guess.
    size_t bufsize = 4096;
    // I think 128 MB ought to be enough.
    while (bufsize <= (1 << 17)) {
        scoped_array_t<char> buf(bufsize);

        struct group g;
        struct group *result;
        int res;
        do {
            res = getgrnam_r(name, &g, buf.data(), bufsize, &result);
        } while (res == EINTR);

        if (res == 0) {
            if (result == nullptr) {
                return false;
            } else {
                *group_id_out = result->gr_gid;
                return true;
            }
        } else if (res == ERANGE) {
            bufsize *= 2;
        } else {
            // Here's what the man page says about error codes of getgrnam_r:
            // 0 or ENOENT or ESRCH or EBADF or EPERM or ...
            //        The given name or uid was not found.
            //
            // So let's just return false here.  I'm sure EMFILE and ENFILE and
            // EIO will turn up again somewhere else.
            return false;
        }
    }

    rassert(false, "get_group_id bufsize overflow");
    return false;
}

// Returns false if the user was not found.  This function replaces a call to
// getpwnam(3).  That's right, getpwnam_r's interface is such that you have to
// go through these shenanigans.
bool get_user_ids(const char *name, uid_t *user_id_out, gid_t *user_group_id_out) {
    // On Linux we can use sysconf to learn what the bufsize should be but on OS
    // X we just have to guess.
    size_t bufsize = 4096;
    // I think 128 MB ought to be enough.
    while (bufsize <= (1 << 17)) {
        scoped_array_t<char> buf(bufsize);

        struct passwd p;
        struct passwd *result;
        int res;
        do {
            res = getpwnam_r(name, &p, buf.data(), bufsize, &result);
        } while (res == EINTR);

        if (res == 0) {
            if (result == nullptr) {
                return false;
            } else {
                *user_id_out = result->pw_uid;
                *user_group_id_out = result->pw_gid;
                return true;
            }
        } else if (res == ERANGE) {
            bufsize *= 2;
        } else {
            // Here's what the man page says about error codes of getpwnam_r:
            // 0 or ENOENT or ESRCH or EBADF or EPERM or ...
            //        The given name or uid was not found.
            //
            // So let's just return false here.  I'm sure EMFILE and ENFILE and
            // EIO will turn up again somewhere else.
            //
            // (Yes, this is the same situation as with getgrnam_r, not some
            // copy/paste.)
            return false;
        }
    }

    rassert(false, "get_user_ids bufsize overflow");
    return false;
}

void get_user_group(const std::map<std::string, options::values_t> &opts,
                    gid_t *group_id_out, std::string *group_name_out,
                    uid_t *user_id_out, std::string *user_name_out) {
    optional<std::string> rungroup = get_optional_option(opts, "--rungroup");
    optional<std::string> runuser = get_optional_option(opts, "--runuser");
    group_name_out->clear();
    user_name_out->clear();

    if (rungroup.has_value()) {
        group_name_out->assign(*rungroup);
        if (!get_group_id(rungroup->c_str(), group_id_out)) {
            throw std::runtime_error(strprintf("Group '%s' not found: %s",
                                               rungroup->c_str(),
                                               errno_string(get_errno()).c_str()).c_str());
        }
    } else {
        *group_id_out = INVALID_GID;
    }

    if (runuser.has_value()) {
        user_name_out->assign(*runuser);
        gid_t user_group_id;
        if (!get_user_ids(runuser->c_str(), user_id_out, &user_group_id)) {
            throw std::runtime_error(strprintf("User '%s' not found: %s",
                                               runuser->c_str(),
                                               errno_string(get_errno()).c_str()).c_str());
        }
        if (!rungroup.has_value()) {
            // No group specified, use the user's group
            group_name_out->assign(*runuser);
            *group_id_out = user_group_id;
        }
    } else {
        *user_id_out = INVALID_UID;
    }
}

void set_user_group(gid_t group_id, const std::string &group_name,
                    uid_t user_id, const std::string user_name) {
    if (group_id != INVALID_GID) {
        if (setgid(group_id) != 0) {
            throw std::runtime_error(strprintf("Could not set group to '%s': %s",
                                               group_name.c_str(),
                                               errno_string(get_errno()).c_str()).c_str());
        }
    }

    if (user_id != INVALID_UID) {
        if (setuid(user_id) != 0) {
            throw std::runtime_error(strprintf("Could not set user account to '%s': %s",
                                               user_name.c_str(),
                                               errno_string(get_errno()).c_str()).c_str());
        }
    }
}

void get_and_set_user_group(const std::map<std::string, options::values_t> &opts) {
    std::string group_name, user_name;
    gid_t group_id;
    uid_t user_id;

    get_user_group(opts, &group_id, &group_name, &user_id, &user_name);
    set_user_group(group_id, group_name, user_id, user_name);
}

void get_and_set_user_group_and_directory(
        const std::map<std::string, options::values_t> &opts,
        directory_lock_t *directory_lock) {
    std::string group_name, user_name;
    gid_t group_id;
    uid_t user_id;

    get_user_group(opts, &group_id, &group_name, &user_id, &user_name);
    directory_lock->change_ownership(group_id, group_name, user_id, user_name);
    set_user_group(group_id, group_name, user_id, user_name);
}
#endif

int check_pid_file(const std::map<std::string, options::values_t> &opts) {
    optional<std::string> pid_filepath = get_optional_option(opts, "--pid-file");
    if (!pid_filepath.has_value() || pid_filepath->empty()) {
        return EXIT_SUCCESS;
    }

    return check_pid_file(*pid_filepath);
}

// Maybe writes a pid file, using the --pid-file option, if it's present.
int write_pid_file(const std::map<std::string, options::values_t> &opts) {
    optional<std::string> pid_filepath = get_optional_option(opts, "--pid-file");
    if (!pid_filepath.has_value() || pid_filepath->empty()) {
        return EXIT_SUCCESS;
    }

    return write_pid_file(*pid_filepath);
}

// Extracts an option that must appear exactly once.  (This is often used for optional arguments
// that have a default value.)
std::string get_single_option(const std::map<std::string, options::values_t> &opts,
                              const std::string &name,
                              std::string *source_out) {
    std::string source;
    optional<std::string> value = get_optional_option(opts, name, &source);

    if (!value.has_value()) {
        throw std::logic_error(strprintf("Missing option '%s'.  (It does not have a default value.)", name.c_str()));
    }

    *source_out = source;
    return *value;
}

std::string get_single_option(const std::map<std::string, options::values_t> &opts,
                              const std::string &name) {
    std::string source;
    return get_single_option(opts, name, &source);
}

// Used for options that don't take parameters, such as --help or --exit-failure, tells whether the
// option exists.
bool exists_option(const std::map<std::string, options::values_t> &opts, const char *name) {
    auto it = opts.find(name);
    return it != opts.end() && !it->second.values.empty();
}

#ifdef _WIN32
std::string windows_version_string() {
    // TODO WINDOWS: the return value of GetVersion may be capped,
    // see https://msdn.microsoft.com/en-us/library/dn481241(v=vs.85).aspx
    DWORD version = GetVersion();
    int major = LOBYTE(LOWORD(version));
    int minor = HIBYTE(LOWORD(version));
    int build = version < 0x80000000 ?  HIWORD(version) : 0;
    std::string name;
    switch (LOWORD(version)) {
    case 0x000A: name ="Windows 10, Server 2016"; break;
    case 0x0306: name ="Windows 8.1, Server 2012"; break;
    case 0x0206: name ="Windows 8, Server 2012"; break;
    case 0x0106: name ="Windows 7, Server 2008 R2"; break;
    case 0x0006: name ="Windows Vista, Server 2008"; break;
    case 0x0205: name ="Windows XP 64-bit, Server 2003"; break;
    case 0x0105: name ="Windows XP"; break;
    case 0x0005: name ="Windows 2000"; break;
    default: name = "Unknown";
    }
    return strprintf("%d.%d.%d (%s)", major, minor, build, name.c_str());
}
#else
// WARNING WARNING WARNING blocking
// if in doubt, DO NOT USE.
std::string run_uname(const std::string &flags) {
    char buf[1024];
    static const std::string unknown = "unknown operating system\n";
    const std::string combined = "uname -" + flags;
    FILE *out = popen(combined.c_str(), "r");
    if (!out) return unknown;
    if (!fgets(buf, sizeof(buf), out)) {
        pclose(out);
        return unknown;
    }
    pclose(out);
    return buf;
}

std::string uname_msr() {
    return run_uname("msr");
}
#endif

void print_version_message() {
    printf("%s\n", RETHINKDB_VERSION_STR);

#ifdef _WIN32
    printf("%s", windows_version_string().c_str());
#else
    printf("%s", uname_msr().c_str());
#endif
}

bool handle_help_or_version_option(const std::map<std::string, options::values_t> &opts,
                                   void (*help_fn)()) {
    if (exists_option(opts, "--help")) {
        help_fn();
        return true;
    } else if (exists_option(opts, "--version")) {
        print_version_message();
        return true;
    }
    return false;
}

std::string cluster_id_path(const base_path_t &dirpath) {
    return dirpath.path() + "/" + CLUSTER_ID_FILENAME;
}

std::string node_id_path(const base_path_t &dirpath) {
    return dirpath.path() + "/" + NODE_ID_FILENAME;
}

optional<fdb_cluster_id> read_cluster_id_file(const base_path_t &dirpath) {
    std::string filename = cluster_id_path(dirpath);
    std::string error_msg;
    bool not_found;
    scoped_fd_t fd = io_utils::open_file_for_read(filename.c_str(), &error_msg, &not_found);
    if (fd.get() == INVALID_FD) {
        if (not_found) {
            return r_nullopt;
        } else {
            throw std::runtime_error("I/O error when opening cluster_id file '" + filename + "': " + error_msg);
        }
    }

    std::vector<char> data;
    if (!io_utils::try_read_file(filename.c_str(), &data, &error_msg)) {
        throw std::runtime_error("I/O error when reading cluster_id file '" + filename + "': " + error_msg);
    }
    uuid_u cluster_id;
    if (!str_to_uuid(data.data(), data.size(), &cluster_id)) {
        throw std::runtime_error("Parse failure of cluster_id file '" + filename + "'");
    }
    return make_optional(fdb_cluster_id{cluster_id});
}

fdb_node_id read_node_id_file(const base_path_t &dirpath) {
    std::string filename = node_id_path(dirpath);
    std::string error_msg;
    bool not_found;
    scoped_fd_t fd = io_utils::open_file_for_read(filename.c_str(), &error_msg, &not_found);
    if (fd.get() == INVALID_FD) {
        if (not_found) {
            throw std::runtime_error("node_id file '" + filename + "' not found");
        } else {
            throw std::runtime_error("I/O error when opening node_id file '" + filename + "': " + error_msg);
        }
    }

    std::vector<char> data;
    if (!io_utils::try_read_file(filename.c_str(), &data, &error_msg)) {
        throw std::runtime_error("I/O error when reading node_id file '" + filename + "': " + error_msg);
    }
    uuid_u node_id;
    if (!str_to_uuid(data.data(), data.size(), &node_id)) {
        throw std::runtime_error("Parse failure of node_id file '" + filename + "'");
    }
    return fdb_node_id{node_id};
}

void initialize_cluster_id_file(const base_path_t &dirpath, const fdb_cluster_id &cluster_id) {
    std::string filename = cluster_id_path(dirpath);
    scoped_fd_t fd = io_utils::create_file(filename.c_str(), true);
    std::string str = uuid_to_str(cluster_id.value);
    std::string error;
    bool res = io_utils::write_all(fd.get(), str.data(), str.size(), &error);
    if (!res) {
        throw std::runtime_error("Failed to write cluster_id file '" + filename + "': " + error);
    }

    warn_fsync_parent_directory(filename.c_str());
}

void initialize_node_id_file(const base_path_t &dirpath, const fdb_node_id &node_id) {
    std::string filename = node_id_path(dirpath);
    scoped_fd_t fd = io_utils::create_file(filename.c_str(), true);
    std::string str = uuid_to_str(node_id.value);
    std::string error;
    bool res = io_utils::write_all(fd.get(), str.data(), str.size(), &error);
    if (!res) {
        throw std::runtime_error("Failed to write node_id file '" + filename + "': " + error);
    }

    warn_fsync_parent_directory(filename.c_str());
}

// Returns the string we pass to fdb_create_database.
std::string get_fdb_client_cluster_file_param(
        const base_path_t &dirpath, const std::map<std::string, options::values_t> &opts) {
    std::string filepath;
    if (exists_option(opts, "--fdb-cluster-file")) {
        return get_single_option(opts, "--fdb-cluster-file");
    } else {
        std::string ret = dirpath.path() + "/" + FDB_CLUSTER_DEFAULT_FILENAME;
        if (!is_rw_file(ret)) {
            // TODO: Maybe precisely allow RW files and noent, fail on any weird stuff.
            ret = "";
        }
        return ret;
    }
}

void initialize_logfile(const std::map<std::string, options::values_t> &opts,
                        const base_path_t &dirpath) {
    std::string filename;
    if (exists_option(opts, "--log-file")) {
        filename = get_single_option(opts, "--log-file");
    } else {
        filename = dirpath.path() + "/" + LOG_FILE_DEFAULT_FILENAME;
    }
    install_fallback_log_writer(filename);
}

std::string get_web_path(optional<std::string> web_static_directory) {
    path_t result;

    if (web_static_directory.has_value()) {
        result = parse_as_path(*web_static_directory);
    } else {
        return std::string();
    }

    // Make sure we return an absolute path
    base_path_t abs_path(render_as_path(result));

    if (!check_existence(abs_path)) {
        throw std::runtime_error(strprintf("ERROR: web assets directory not found '%s'",
                                           abs_path.path().c_str()).c_str());
    }

    abs_path = abs_path.make_absolute();
    return abs_path.path();
}

std::string get_web_path(const std::map<std::string, options::values_t> &opts) {
    if (!exists_option(opts, "--no-http-admin")) {
        optional<std::string> web_static_directory = get_optional_option(opts, "--web-static-directory");
        return get_web_path(web_static_directory);
    }
    return std::string();
}

optional<int> parse_join_delay_secs_option(
        const std::map<std::string, options::values_t> &opts) {
    if (exists_option(opts, "--join-delay")) {
        const std::string delay_opt = get_single_option(opts, "--join-delay");
        uint64_t join_delay_secs;
        if (!strtou64_strict(delay_opt, 10, &join_delay_secs)) {
            throw std::runtime_error(strprintf(
                    "ERROR: join-delay should be a number, got '%s'",
                    delay_opt.c_str()));
        }
        if (join_delay_secs > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            throw std::runtime_error(strprintf(
                    "ERROR: join-delay is too large. Must be at most %d",
                    std::numeric_limits<int>::max()));
        }
        return optional<int>(static_cast<int>(join_delay_secs));
    } else {
        return optional<int>();
    }
}

optional<int> parse_node_reconnect_timeout_secs_option(
        const std::map<std::string, options::values_t> &opts) {
    if (exists_option(opts, "--cluster-reconnect-timeout")) {
        const std::string timeout_opt = get_single_option(opts, "--cluster-reconnect-timeout");
        uint64_t node_reconnect_timeout_secs;
        if (!strtou64_strict(timeout_opt, 10, &node_reconnect_timeout_secs)) {
            throw std::runtime_error(strprintf(
                    "ERROR: cluster-reconnect-timeout should be a number, got '%s'",
                    timeout_opt.c_str()));
        }
        if (node_reconnect_timeout_secs > static_cast<uint64_t>(std::numeric_limits<int>::max()) ||
            node_reconnect_timeout_secs * 1000 > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            throw std::runtime_error(strprintf(
                "ERROR: cluster-reconnect-timeout is too large. Must be at most %d",
                std::numeric_limits<int>::max() / 1000));
        }
        return optional<int>(static_cast<int>(node_reconnect_timeout_secs));
    }

    return optional<int>();
}

/* An empty outer `optional` means the `--cache-size` parameter is not present. An
empty inner `optional` means the cache size is set to `auto`. */
optional<optional<uint64_t> > parse_total_cache_size_option(
        const std::map<std::string, options::values_t> &opts) {
    if (exists_option(opts, "--cache-size")) {
        const std::string cache_size_opt = get_single_option(opts, "--cache-size");
        if (cache_size_opt == "auto") {
            return optional<optional<uint64_t> >(
                optional<uint64_t>());
        } else {
            uint64_t cache_size_megs;
            if (!strtou64_strict(cache_size_opt, 10, &cache_size_megs)) {
                throw std::runtime_error(strprintf(
                        "ERROR: cache-size should be a number or 'auto', got '%s'",
                        cache_size_opt.c_str()));
            }
            const uint64_t res = cache_size_megs * MEGABYTE;
            if (res > get_max_total_cache_size()) {
                throw std::runtime_error(strprintf(
                    "ERROR: cache-size is %" PRIu64 " MB, which is larger than the "
                    "maximum legal cache size for this platform (%" PRIu64 " MB)",
                    res, get_max_total_cache_size()));
            }
            return optional<optional<uint64_t> >(
                optional<uint64_t>(res));
        }
    } else {
        return make_optional(optional<uint64_t>());
    }
}

// Note that this defaults to the peer port if no port is specified
//  (at the moment, this is only used for parsing --join directives)
// Possible formats:
//  host only: newton
//  host and port: newton:60435
//  IPv4 addr only: 192.168.0.1
//  IPv4 addr and port: 192.168.0.1:60435
//  IPv6 addr only: ::dead:beef
//  IPv6 addr only: [::dead:beef]
//  IPv6 addr and port: [::dead:beef]:60435
//  IPv4-mapped IPv6 addr only: ::ffff:192.168.0.1
//  IPv4-mapped IPv6 addr only: [::ffff:192.168.0.1]
//  IPv4-mapped IPv6 addr and port: [::ffff:192.168.0.1]:60435
host_and_port_t parse_host_and_port(const std::string &source, const std::string &option_name,
                                    const std::string &value, port_t default_port) {
    // First disambiguate IPv4 vs IPv6
    size_t colon_count = std::count(value.begin(), value.end(), ':');

    if (colon_count < 2) {
        // IPv4 will have 1 or less colons
        size_t colon_loc = value.find_last_of(':');
        if (colon_loc != std::string::npos) {
            std::string host = value.substr(0, colon_loc);
            int port = atoi(value.substr(colon_loc + 1).c_str());
            if (host.size() != 0 && port > 0 && port <= MAX_PORT) {
                return host_and_port_t(host, port_t{static_cast<uint16_t>(port)});
            }
        } else if (value.size() != 0) {
            return host_and_port_t(value, default_port);
        }
    } else {
        // IPv6 will have 2 or more colons
        size_t last_colon_loc = value.find_last_of(':');
        size_t start_bracket_loc = value.find_first_of('[');
        size_t end_bracket_loc = value.find_last_of(']');

        if (start_bracket_loc > end_bracket_loc) {
            // Error condition fallthrough
        } else if (start_bracket_loc == std::string::npos || end_bracket_loc == std::string::npos) {
            // No brackets, therefore no port, just parse the whole thing as a hostname
            return host_and_port_t(value, default_port);
        } else if (last_colon_loc < end_bracket_loc) {
            // Brackets, but no port, verify no other characters outside the brackets
            if (value.find_last_not_of(" \t\r\n[", start_bracket_loc) == std::string::npos &&
                value.find_first_not_of(" \t\r\n]", end_bracket_loc) == std::string::npos) {
                std::string host = value.substr(start_bracket_loc + 1, end_bracket_loc - start_bracket_loc - 1);
                return host_and_port_t(host, default_port);
            }
        } else {
            // Brackets and port
            std::string host = value.substr(start_bracket_loc + 1, end_bracket_loc - start_bracket_loc - 1);
            std::string remainder = value.substr(end_bracket_loc + 1);
            size_t remainder_colon_loc = remainder.find_first_of(':');
            int port = atoi(remainder.substr(remainder_colon_loc + 1).c_str());

            // Verify no characters before the brackets and up to the port colon
            if (port > 0 && port <= MAX_PORT && remainder_colon_loc == 0 &&
                value.find_last_not_of(" \t\r\n[", start_bracket_loc) == std::string::npos) {
                return host_and_port_t(host, port_t{static_cast<uint16_t>(port)});
            }
        }
    }


    throw options::value_error_t(source, option_name,
                                 strprintf("Option '%s' has invalid host and port format '%s'",
                                           option_name.c_str(), value.c_str()));
}

class address_lookup_exc_t : public std::exception {
public:
    explicit address_lookup_exc_t(const std::string& data) : info(data) { }
    ~address_lookup_exc_t() throw () { }
    const char *what() const throw () { return info.c_str(); }
private:
    std::string info;
};

std::set<ip_address_t> get_local_addresses(
    const std::vector<std::string> &specific_options,
    const std::vector<std::string> &default_options,
    local_ip_filter_t filter_type) {
    std::set<ip_address_t> set_filter;

    const std::vector<std::string> &bind_options =
        specific_options.size() > 0 ?
        specific_options :
        default_options;

    // Scan through specified bind options
    for (size_t i = 0; i < bind_options.size(); ++i) {
        if (bind_options[i] == "all") {
            filter_type = local_ip_filter_t::ALL;
        } else {
            // Verify that all specified addresses are valid ip addresses
            try {
                ip_address_t addr(bind_options[i]);
                if (addr.is_any()) {
                    filter_type = local_ip_filter_t::ALL;
                } else {
                    set_filter.insert(addr);
                }
            } catch (const std::exception &ex) {
                throw address_lookup_exc_t(strprintf("bind ip address '%s' could not be parsed: %s",
                                                     bind_options[i].c_str(),
                                                     ex.what()));
            }
        }
    }

    std::set<ip_address_t> result = get_local_ips(set_filter, filter_type);

    // Make sure that all specified addresses were found
    for (std::set<ip_address_t>::iterator i = set_filter.begin(); i != set_filter.end(); ++i) {
        if (result.find(*i) == result.end()) {
            std::string errmsg = strprintf("Could not find bind ip address '%s'", i->to_string().c_str());
            if (i->is_ipv6_link_local()) {
                errmsg += strprintf(", this is an IPv6 link-local address, make sure the scope is correct");
            }
            throw address_lookup_exc_t(errmsg);
        }
    }

    if (result.empty()) {
        throw address_lookup_exc_t("No local addresses found to bind to.");
    }

    // If we will use all local addresses, return an empty set, which is how tcp_listener_t does it
    if (filter_type == local_ip_filter_t::ALL) {
        return std::set<ip_address_t>();
    }

    return result;
}

// Returns the options vector for a given option name.  The option must *exist*!  Typically this is
// for OPTIONAL_REPEAT options with a default value being the empty vector.
const std::vector<std::string> &all_options(const std::map<std::string, options::values_t> &opts,
                                            const std::string &name,
                                            std::string *source_out) {
    auto it = opts.find(name);
    if (it == opts.end()) {
        throw std::logic_error(strprintf("option '%s' not found", name.c_str()));
    }
    *source_out = it->second.source;
    return it->second.values;
}

const std::vector<std::string> &all_options(const std::map<std::string, options::values_t> &opts,
                                            const std::string &name) {
    std::string source;
    return all_options(opts, name, &source);
}

// Gets a single integer option, often an optional integer option with a default value.
int get_single_int(const std::map<std::string, options::values_t> &opts, const std::string &name) {
    const std::string value = get_single_option(opts, name);
    int64_t x;
    if (strtoi64_strict(value, 10, &x)) {
        if (INT_MIN <= x && x <= INT_MAX) {
            return x;
        }
    }
    throw std::runtime_error(strprintf("Option '%s' (with value '%s') not a valid integer",
                                       name.c_str(), value.c_str()));
}

port_t offseted_port(const char *name, const int port, const int port_offset) {
    int ret = port == 0 ? 0 : port + port_offset;
    if (ret > 0 && ret <= MAX_PORT) {
        return port_t{static_cast<uint16_t>(ret)};
    }
    throw invalid_port_exc_t(name, ret, port_offset);
}

peer_address_t get_canonical_addresses(const std::map<std::string, options::values_t> &opts,
                                       port_t default_port) {
    std::string source;
    std::vector<std::string> canonical_options = all_options(opts, "--canonical-address", &source);
    // Verify that all specified addresses are valid ip addresses
    std::set<host_and_port_t> result;
    for (size_t i = 0; i < canonical_options.size(); ++i) {
        host_and_port_t host_port = parse_host_and_port(source, "--canonical-address",
                                                        canonical_options[i], default_port);

        if (host_port.port().value == 0 && default_port.value != 0) {
            // The cluster layer would probably swap this out with whatever port we were
            //  actually listening on, but since the user explicitly specified 0, it doesn't make sense
            throw std::logic_error("cannot specify a port of 0 in --canonical-address");
        }
        result.insert(host_port);
    }
    return peer_address_t(result);
}

service_address_ports_t get_service_address_ports(const std::map<std::string, options::values_t> &opts) {
    const int port_offset = get_single_int(opts, "--port-offset");
    // Passing "port" string because we blindly moved logic from service_address_ports_t ctor.
    // const int cluster_port = offseted_port("port", get_single_int(opts, "--cluster-port"), port_offset);
    // const int client_port = get_single_int(opts, "--client-port");

    local_ip_filter_t filter =
        exists_option(opts, "--no-default-bind") ?
            local_ip_filter_t::MATCH_FILTER :
            local_ip_filter_t::MATCH_FILTER_OR_LOOPBACK;

    const std::vector<std::string> &default_options =
        all_options(opts, "--bind");
    return service_address_ports_t(
        get_local_addresses(default_options, default_options, filter),
        // We don't have this in ReFound... yet.
        /* get_local_addresses(all_options(opts, "--bind-cluster"),
                            default_options,
                            filter), */
        get_local_addresses(all_options(opts, "--bind-driver"),
                            default_options,
                            filter),
        get_local_addresses(all_options(opts, "--bind-http"),
                            default_options,
                            filter),
        /* get_canonical_addresses(opts, cluster_port), */
        /* cluster_port, */
        /* client_port, */
        exists_option(opts, "--no-http-admin"),
        // Using "http_port" and "reql_port" spellings because we blindly moved logic from
        // service_address_ports_t ctor.
        offseted_port("http_port", get_single_int(opts, "--http-port"), port_offset),
        offseted_port("reql_port", get_single_int(opts, "--driver-port"), port_offset),
        port_offset);
}

#ifdef ENABLE_TLS
bool load_tls_key_and_cert(
    SSL_CTX *tls_ctx, const std::string &key_file, const std::string &cert_file) {
    if(SSL_CTX_use_PrivateKey_file(tls_ctx, key_file.c_str(), SSL_FILETYPE_PEM) <= 0) {
        ERR_print_errors_fp(stderr);
        return false;
    }

    if(SSL_CTX_use_certificate_chain_file(tls_ctx, cert_file.c_str()) <= 0) {
        ERR_print_errors_fp(stderr);
        return false;
    }

    if(1 != SSL_CTX_check_private_key(tls_ctx)) {
        logERR("The private key and certificate do not match.");
        return false;
    }

    return true;
}

bool configure_web_tls(
    const std::map<std::string, options::values_t> &opts, SSL_CTX *web_tls) {
    optional<std::string> key_file = get_optional_option(opts, "--http-tls-key");
    optional<std::string> cert_file = get_optional_option(opts, "--http-tls-cert");

    if (!(key_file.has_value() && cert_file.has_value())) {
        logERR("--http-tls-key and --http-tls-cert must be specified together.");
        return false;
    }

    return load_tls_key_and_cert(web_tls, *key_file, *cert_file);
}

bool configure_driver_tls(
    const std::map<std::string, options::values_t> &opts, SSL_CTX *driver_tls) {
    optional<std::string> key_file = get_optional_option(
        opts, "--driver-tls-key");
    optional<std::string> cert_file = get_optional_option(
        opts, "--driver-tls-cert");
    optional<std::string> ca_file = get_optional_option(opts, "--driver-tls-ca");

    if (!(key_file.has_value() && cert_file.has_value())) {
        if (key_file.has_value() || cert_file.has_value()) {
            logERR("--driver-tls-key and --driver-tls-cert must be specified together.");
        } else {
            rassert(ca_file.has_value());
            logERR("--driver-tls-key and --driver-tls-cert must be specified if "
                   "--driver-tls-ca is specified.");
        }
        return false;
    }

    if (!load_tls_key_and_cert(driver_tls, *key_file, *cert_file)) {
        return false;
    }

    if (ca_file.has_value()) {
        if (!SSL_CTX_load_verify_locations(driver_tls, ca_file->c_str(), nullptr)) {
            ERR_print_errors_fp(stderr);
            return false;
        }

        // Mutual authentication.
        SSL_CTX_set_verify(
            driver_tls, SSL_VERIFY_PEER|SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    }

    return true;
}

bool configure_cluster_tls(
    const std::map<std::string, options::values_t> &opts, SSL_CTX *cluster_tls) {
    optional<std::string> key_file = get_optional_option(
        opts, "--cluster-tls-key");
    optional<std::string> cert_file = get_optional_option(
        opts, "--cluster-tls-cert");
    optional<std::string> ca_file = get_optional_option(opts, "--cluster-tls-ca");

    if (!(key_file.has_value() && cert_file.has_value() && ca_file.has_value())) {
        logERR("--cluster-tls-key, --cluster-tls-cert, and --cluster-tls-ca must be "
               "specified together.");
        return false;
    }

    if (!load_tls_key_and_cert(cluster_tls, *key_file, *cert_file)) {
        return false;
    }

    if (!SSL_CTX_load_verify_locations(cluster_tls, ca_file->c_str(), nullptr)) {
        ERR_print_errors_fp(stderr);
        return false;
    }

    // Mutual authentication.
    SSL_CTX_set_verify(
        cluster_tls, SSL_VERIFY_PEER|SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);

    return true;
}

class fp_wrapper_t {
public:
    fp_wrapper_t(const char *filename, const char *mode) {
        fp = fopen(filename, mode);
    }

    ~fp_wrapper_t() {
        if (fp != nullptr) {
            fclose(fp);
        }
    }

    FILE *get() {
        return fp;
    }

private:
    FILE *fp;
};

bool initialize_tls_ctx(
    const std::map<std::string, options::values_t> &opts,
    shared_ssl_ctx_t *tls_ctx_out) {

    tls_ctx_out->reset(SSL_CTX_new(SSLv23_method()), SSL_CTX_free);
    if (nullptr == tls_ctx_out->get()) {
        ERR_print_errors_fp(stderr);
        return false;
    }

    optional<std::string> min_protocol_opt = get_optional_option(
        opts, "--tls-min-protocol");
    long protocol_flags = // NOLINT(runtime/int)
        SSL_OP_NO_SSLv2|SSL_OP_NO_SSLv3|SSL_OP_NO_TLSv1|SSL_OP_NO_TLSv1_1;
    if (min_protocol_opt.has_value()) {
        if (*min_protocol_opt == "TLSv1") {
            protocol_flags ^= SSL_OP_NO_TLSv1|SSL_OP_NO_TLSv1_1;
        } else if (*min_protocol_opt == "TLSv1.1") {
            protocol_flags ^= SSL_OP_NO_TLSv1_1;
        } else if (*min_protocol_opt == "TLSv1.2") {
            // Already the default
        } else {
            logERR("Unrecognized TLS protocol version '%s'.", min_protocol_opt->c_str());
            return false;
        }
    }
    SSL_CTX_set_options(tls_ctx_out->get(), protocol_flags);

    // Prefer server ciphers, and always generate new keys for DHE or ECDHE.
    SSL_CTX_set_options(
        tls_ctx_out->get(),
        SSL_OP_CIPHER_SERVER_PREFERENCE|SSL_OP_SINGLE_DH_USE|SSL_OP_SINGLE_ECDH_USE);

    /* This is pretty important. We want to use the most secure TLS cipher
    suite that we can. Our default list only allows ciphers suites which employ
    ECDHE (Elliptic Curve Diffie-Hellman with Ephemeral keys) for encryption
    key agreement for Perfect Forward Secrecy and we want to use AESGCM for
    efficient authenticated encryption. An option exists for system admins to
    configure their own ciphers list if their SSL/TLS lib doesn't support these
    options or if they want to use other cipher suites. */
    optional<std::string> ciphers_opt = get_optional_option(
        opts, "--tls-ciphers");
    /* NOTE: I normally prefer the abreviation 'ECDHE', but older versions of
    OpenSSL do not seem to like 'ECDHE' even though the matching cipher suites
    have that prefix. Yeah, I know... it's very frustrating. Both older (1.0.1)
    and newer (1.0.2) versions seem to be okay with 'EECDH' though. */
    std::string ciphers = ciphers_opt.has_value() ? *ciphers_opt : "EECDH+AESGCM";
    if (0 == SSL_CTX_set_cipher_list(tls_ctx_out->get(), ciphers.c_str())) {
        logERR("No secure cipher suites available.\n");
        return false;
    }

    /* The default curve name corresponds to a standard NIST/FIPS curve and is
    also commonly known as P-256. Some people speculate that the NIST/FIPS
    curves are compromised in some way by the NSA but most people agree that
    they are safe to use. Any real risk in using elliptic curves is associated
    with ECDSA (Elliptic Curve Digital Signature Algorithm) where you must be
    careful to generate signatures correctly using a secure PRNG. As we are
    using them only for ECDHE, this should be safe. This is just our default
    ECDHE curve and is widley supported by TLS libraries, but a system admin
    may specify any preferred curve which is supported by their local version
    of libssl with a command line option - for example 'secp256k1' (widely used
    and popularized by the Bitcoin cryptocurrency) and Daniel J. Bernstein's
    'Curve25519' (though not currently supported by OpenSSL) would be other
    good choices.
    NOTE: For compatibility reasons, we can only support a single elliptic
    curve since OpenSSL is ridiculous. */
    optional<std::string> curve_opt = get_optional_option(
        opts, "--tls-ecdh-curve");
    std::string curve_name = curve_opt.has_value() ? *curve_opt : "prime256v1";

    int curve_nid = OBJ_txt2nid(curve_name.c_str());
    if (NID_undef == curve_nid) {
        logERR("No elliptic curve found corresponding to name '%s'.", curve_name.c_str());
        return false;
    }

    EC_KEY *ec_key = EC_KEY_new_by_curve_name(curve_nid);
    if (nullptr == ec_key) {
        logERR("Unable to get elliptic curve by name '%s'.", curve_name.c_str());
        return false;
    }

    SSL_CTX_set_tmp_ecdh(tls_ctx_out->get(), ec_key);
    EC_KEY_free(ec_key);


    /* If we only supported OpenSSL 1.0.2 then we could have multiple ECDHE
    curves with something like this:

    if (0 == SSL_CTX_set1_curves_list(tls_ctx_out->get(), curves.c_str())) {
        logERR("Not able to set ECDHE curves for perfect forward secrecy\n");
        return false;
    }

    // Enabling this option allows the server to select the appropriate curve
    // that is shared with the client.
    if (0 == SSL_CTX_set_ecdh_auto(tls_ctx_out->get(), 1)) {
        logERR("Not able to set ECDHE curve selection mode\n");
        return false;
    }

    */

    /* If the client and server do not support ECDHE but do support DHE, an
    admin must specify a file containing parameters for DHE. */
    optional<std::string> dhparams_filename = get_optional_option(
        opts, "--tls-dhparams");
    if (dhparams_filename.has_value()) {
        fp_wrapper_t dhparams_fp(dhparams_filename->c_str(), "r");
        if (nullptr == dhparams_fp.get()) {
            logERR(
                "Unable to open '%s' for reading: %s",
                dhparams_filename->c_str(),
                errno_string(get_errno()).c_str());
            return false;
        }

        DH *dhparams = PEM_read_DHparams(
            dhparams_fp.get(), nullptr, nullptr, nullptr);
        if (nullptr == dhparams) {
            unsigned long err_code = ERR_get_error(); // NOLINT(runtime/int)
            const char *err_str = ERR_reason_error_string(err_code);
            logERR(
                "Unable to read DH parameters from '%s': %s (OpenSSL error %lu)",
                dhparams_filename->c_str(),
                err_str == nullptr ? "unknown error" : err_str,
                err_code);
            return false;
        }

        if (1 != SSL_CTX_set_tmp_dh(tls_ctx_out->get(), dhparams)) {
            unsigned long err_code = ERR_get_error(); // NOLINT(runtime/int)
            const char *err_str = ERR_reason_error_string(err_code);
            logERR(
                "Unable to set DH parameters: %s (OpenSSL error %lu)",
                err_str == nullptr ? "unknown error" : err_str,
                err_code);
            return false;
        }
    }

    return true;
}

bool configure_tls(
    const std::map<std::string, options::values_t> &opts,
    tls_configs_t *tls_configs_out) {

    if(!exists_option(opts, "--no-http-admin") &&
            (exists_option(opts, "--http-tls-key")
             || exists_option(opts, "--http-tls-cert"))) {
        if (!(initialize_tls_ctx(opts, &(tls_configs_out->web)) &&
              configure_web_tls(opts, tls_configs_out->web.get())
            )) {
            return false;
        }
    }

    if (exists_option(opts, "--driver-tls-key")
        || exists_option(opts, "--driver-tls-cert")
        || exists_option(opts, "--driver-tls-ca")) {
        if (!(initialize_tls_ctx(opts, &(tls_configs_out->driver)) &&
              configure_driver_tls(opts, tls_configs_out->driver.get())
            )) {
            return false;
        }
    }

    if (exists_option(opts, "--cluster-tls-key")
        || exists_option(opts, "--cluster-tls-cert")
        || exists_option(opts, "--cluster-tls-ca")) {
        if (!(initialize_tls_ctx(opts, &(tls_configs_out->cluster)) &&
              configure_cluster_tls(opts, tls_configs_out->cluster.get())
            )) {
            return false;
        }
    }

    return true;
}
#endif /* ENABLE_TLS */


// OOO: Remove config_version_exc_t?

MUST_USE bool set_initial_password(FDBTransaction *txn, const signal_t *non_interruptor, const std::string &initial_password) {
    guarantee(!initial_password.empty());
    fdb_value_fut<auth::user_t> user_fut
        = transaction_get_user(txn, auth::username_t("admin"));
    auth::user_t user;
    if (!user_fut.block_and_deserialize(non_interruptor, &user)) {
        crash("admin user not found in fdb");  // TODO: fdb error, msg, etc.
    }
    bool needs_configuration = user.get_password().is_empty();
    if (needs_configuration) {
        uint32_t iterations = auth::password_t::default_iteration_count;
        auth::password_t pw(initial_password, iterations);
        auth::user_t new_user = auth::user_t(std::move(pw));
        transaction_modify_user(txn, auth::username_t("admin"),
            user, new_user);
    }
    return needs_configuration;
}

constexpr const char *initial_password_msg =
    "Ignoring --initial-password option because the admin "
    "password is already configured.";

void run_rethinkdb_serve(FDBDatabase *fdb,
                         const base_path_t &base_path,
                         const serve_info_t *serve_info,
                         const std::string &initial_password,
                         directory_lock_t *data_directory_lock,
                         bool *const result_out) {
    logNTC("Running %s...\n", RETHINKDB_VERSION_STR);
#ifdef _WIN32
    logNTC("Running on %s", windows_version_string().c_str());
#else
    logNTC("Running on %s", uname_msr().c_str());
#endif
    os_signal_cond_t sigint_cond;

    logNTC("Loading data from directory %s\n", base_path.path().c_str());

    try {
        cond_t non_interruptor;
        {
            if (!initial_password.empty()) {
                /* Apply the initial password if there isn't one already. */
                // TODO: Is there some sort of sigint interruptor we can set up earlier?
                bool already_configured = false;
                fdb_error_t loop_err = txn_retry_loop_coro(fdb, &non_interruptor, [&](FDBTransaction *txn) {
                    bool mutated = set_initial_password(txn, &non_interruptor, initial_password);
                    if (mutated) {
                        // We have changes, so commit.
                        commit(txn, &non_interruptor);
                    }
                    already_configured = !mutated;
                });
                if (loop_err != 0) {
                    logERR("FoundationDB error when processing --initial-password option: %s",
                           fdb_get_error(loop_err));
                    *result_out = false;
                    return;
                }

                if (already_configured) {
                    logNTC(initial_password_msg);
                }
            }
        }

        // Tell the directory lock that the directory is now good to go, as it will
        //  otherwise delete an uninitialized directory
        data_directory_lock->directory_initialized();

        *result_out = serve(fdb,
                            *serve_info,
                            &sigint_cond);

    } catch (const host_lookup_exc_t &ex) {
        logERR("%s\n", ex.what());
        *result_out = false;
    }
}

options::help_section_t get_server_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Server options");
    options_out->push_back(options::option_t(options::names_t("--server-name", "-n"),
                                             options::OPTIONAL,
                                             obsolescence::UNSUPPORTED_IGNORED));
    help.add("-n [ --server-name ] arg",
             "the name for this server (as will appear in the metadata).  If not"
             " specified, one will be generated from the hostname and a random "
             "alphanumeric string.  Unsupported (for now) in ReFound.");

    options_out->push_back(options::option_t(options::names_t("--server-tag", "-t"),
                                             options::OPTIONAL_REPEAT,
                                             obsolescence::UNSUPPORTED_IGNORED));
    help.add("-t [ --server-tag ] arg",
             "a tag for this server. Can be specified multiple times.  Unsupported (for now) in ReFound.");

    return help;
}

options::help_section_t get_auth_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Authentication options");

    options_out->push_back(options::option_t(options::names_t("--initial-password"),
                                             options::OPTIONAL));
    help.add("--initial-password {auto | password}",
             "sets an initial password for the \"admin\" user on a new server.  If set "
             "to auto, a random password will be generated. Care should be taken when "
             "using values other than auto as your password can be leaked into system logs "
             "and process monitors. As a safer alternative, create a file with the content "
             "initial-password=Y0urP4$$woRd and load it using the --config-file option.");

    return help;
}

options::help_section_t get_log_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Log options");
    options_out->push_back(options::option_t(options::names_t("--log-file"),
                                             options::OPTIONAL));
    help.add("--log-file file", "specify the file to log to, defaults to 'log_file'");
    options_out->push_back(options::option_t(options::names_t("--no-update-check"),
                                            options::OPTIONAL_NO_PARAMETER));
    help.add("--no-update-check", "obsolete.  Update checking has been removed.");
    return help;
}

options::help_section_t get_fdb_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("FoundationDB options");
    options_out->push_back(options::option_t(options::names_t("--fdb-cluster-file"),
                                             options::OPTIONAL));
    help.add("--fdb-cluster-file file",
        "specify the fdb cluster file to use, defaults to 'fdb.cluster'.  If unspecified, "
        "and if the data dir has no 'fdb.cluster' file, falls back to default "
        "FoundationDB client behavior: The value of the FDB_CLUSTER_FILE environment "
        "variable is used if present, then 'fdb.cluster' in the local working directory "
        "if present, then the default file at its system-dependent location.");
    return help;
}

options::help_section_t get_fdb_create_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("FoundationDB-related creation options");
    options_out->push_back(options::option_t(options::names_t("--fdb-force-wipe"),
                                             options::OPTIONAL_NO_PARAMETER));
    help.add("--fdb-force-wipe",
        "used with --fdb-init.  wipe out FoundationDB on creation, if it has "
        "an existing ReFound instance");
    options_out->push_back(options::option_t(options::names_t("--fdb-init"),
                                             options::OPTIONAL_NO_PARAMETER));
    help.add("--fdb-init", "initialize RethinkDB instance in FoundationDB");
    return help;
}

options::help_section_t get_file_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("File path options");
    options_out->push_back(options::option_t(options::names_t("--directory", "-d"),
                                             options::OPTIONAL,
                                             "rethinkdb_data"));
    help.add("-d [ --directory ] path", "specify directory in which to read and write metadata");
    options_out->push_back(options::option_t(options::names_t("--io-threads"),
                                             options::OPTIONAL,
                                             strprintf("%d", DEFAULT_MAX_CONCURRENT_IO_REQUESTS),
                                             obsolescence::OBSOLETE_IGNORED));
    help.add("--io-threads n",
             "obsolete in ReFound.");
#ifndef _WIN32
    // TODO WINDOWS: accept this option, but error out if it is passed
    options_out->push_back(options::option_t(options::names_t("--direct-io"),
                                             options::OPTIONAL_NO_PARAMETER,
                                             obsolescence::OBSOLETE_IGNORED));
    help.add("--direct-io", "obsolete in ReFound.  use direct I/O for file access");
#endif
    options_out->push_back(options::option_t(options::names_t("--cache-size"),
                                             options::OPTIONAL,
                                             obsolescence::OBSOLETE_IGNORED));
    help.add("--cache-size mb", "obsolete in ReFound.  total cache size (in megabytes) for the process. Can "
        "be 'auto'.");
    return help;
}

options::help_section_t get_config_file_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Configuration file options");
    options_out->push_back(options::option_t(options::names_t("--config-file"),
                                             options::OPTIONAL));
    help.add("--config-file", "take options from a configuration file");
    return help;
}

std::vector<host_and_port_t> parse_join_options(const std::map<std::string, options::values_t> &opts,
                                                port_t default_port) {
    std::string source;
    const std::vector<std::string> join_strings = all_options(opts, "--join", &source);
    std::vector<host_and_port_t> joins;
    for (auto it = join_strings.begin(); it != join_strings.end(); ++it) {
        joins.push_back(parse_host_and_port(source, "--join", *it, default_port));
    }
    return joins;
}

bool parse_init_options(const std::map<std::string, options::values_t> &opts, bool *init_out, bool *wipe_out) {
    *init_out = exists_option(opts, "--fdb-init");
    *wipe_out = exists_option(opts, "--fdb-force-wipe");
    if (*wipe_out && !*init_out) {
        fprintf(stderr, "--fdb-force-wipe cannot be used without --fdb-init\n");
        return false;
    }
    return true;
}

name_string_t parse_server_name_option(
        const std::map<std::string, options::values_t> &opts) {
    optional<std::string> server_name_str =
        get_optional_option(opts, "--server-name");
    if (server_name_str.has_value()) {
        name_string_t server_name;
        if (!server_name.assign_value(*server_name_str)) {
            throw std::runtime_error(strprintf("server-name '%s' is invalid.  (%s)\n",
                    server_name_str->c_str(), name_string_t::valid_char_msg));
        }
        return server_name;
    } else {
        return get_default_server_name();
    }
}

std::set<name_string_t> parse_server_tag_options(
        const std::map<std::string, options::values_t> &opts) {
    std::set<name_string_t> server_tag_names;
    for (const std::string &tag_str : opts.at("--server-tag").values) {
        name_string_t tag;
        if (!tag.assign_value(tag_str)) {
            throw std::runtime_error(strprintf("--server_tag %s is invalid. (%s)\n",
                tag_str.c_str(), name_string_t::valid_char_msg));
        }
        /* We silently accept tags that appear multiple times. */
        server_tag_names.insert(tag);
    }
    server_tag_names.insert(name_string_t::guarantee_valid("default"));
    return server_tag_names;
}

std::string parse_initial_password_option(
        const std::map<std::string, options::values_t> &opts) {
    optional<std::string> initial_password_str =
        get_optional_option(opts, "--initial-password");
    if (initial_password_str.has_value()) {
        if (*initial_password_str == "auto") {
            std::array<unsigned char, 16> random_data = crypto::random_bytes<16>();
            const uuid_u base_uuid = str_to_uuid("4a3a5542-6a45-4668-a09a-d775e63a52cd");
            std::string random_pw = uuid_to_str(uuid_u::from_hash(
                base_uuid,
                std::string(reinterpret_cast<const char *>(random_data.data()), 16)));
            printf("Generated random admin password: %s\n", random_pw.c_str());
            return random_pw;
        }
        return *initial_password_str;
    } else {
        // The default is an empty admin password
        return "";
    }
}

std::string get_reql_http_proxy_option(const std::map<std::string, options::values_t> &opts) {
    std::string source;
    optional<std::string> proxy = get_optional_option(opts, "--reql-http-proxy", &source);
    if (!proxy.has_value()) {
        return std::string();
    }

    // We verify the correct format here, as we won't be configuring libcurl until later,
    // in the extprocs.  At the moment, we do not support specifying IPv6 addresses.
    //
    // protocol = proxy protocol recognized by libcurl: (http, socks4, socks4a, socks5, socks5h)
    // host = hostname or ip address
    // port = integer in range [0-65535]
    // [protocol://]host[:port]
    //
    // The chunks in the regex used to parse and verify the format are:
    //   protocol - (?:([A-z][A-z0-9+-.]*)(?:://))? - captures the protocol, adhering to
    //     RFC 3986, discarding the '://' from the end
    //   host - ([A-z0-9.-]+) - captures the hostname or ip address, consisting of letters,
    //     numbers, dashes, and dots
    //   port - (?::(\d+))? - captures the numeric port, discarding the preceding ':'
    RE2 re2_parser("(?:([A-z][A-z0-9+-.]*)(?:://))?([A-z0-9_.-]+)(?::(\\d+))?",
                   RE2::Quiet);
    std::string protocol, host, port_str;
    if (!RE2::FullMatch(proxy.get(), re2_parser, &protocol, &host, &port_str)) {
        throw std::runtime_error(strprintf("--reql-http-proxy format unrecognized, "
                                           "expected [protocol://]host[:port]: %s",
                                           proxy.get().c_str()));
    }

    if (!protocol.empty() &&
        protocol != "http" &&
        protocol != "socks4" &&
        protocol != "socks4a" &&
        protocol != "socks5" &&
        protocol != "socks5h") {
        throw std::runtime_error(strprintf("--reql-http-proxy protocol unrecognized (%s), "
                                           "must be one of http, socks4, socks4a, socks5, "
                                           "and socks5h", protocol.c_str()));
    }

    if (!port_str.empty()) {
        int port = atoi(port_str.c_str());
        if (port_str.length() > 5 || port <= 0 || port > MAX_PORT) {
            throw std::runtime_error(strprintf("--reql-http-proxy port (%s) is not in "
                                               "the valid range (0-65535)",
                                               port_str.c_str()));
        }
    }
    return proxy.get();
}

options::help_section_t get_web_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Web options");
    options_out->push_back(options::option_t(options::names_t("--web-static-directory"),
                                             options::OPTIONAL));
    help.add("--web-static-directory directory", "the directory containing web resources for the http interface");
    options_out->push_back(options::option_t(options::names_t("--http-port"),
                                             options::OPTIONAL,
                                             strprintf("%d", port_defaults::http_port.value)));
    help.add("--http-port port", "port for web administration console");
    options_out->push_back(options::option_t(options::names_t("--no-http-admin"),
                                             options::OPTIONAL_NO_PARAMETER));
    help.add("--no-http-admin", "disable web administration console");
    return help;
}

options::help_section_t get_network_options(const bool join_required, std::vector<options::option_t> *options_out) {
    options::help_section_t help("Network options");
    options_out->push_back(options::option_t(options::names_t("--bind"),
                                             options::OPTIONAL_REPEAT));
    options_out->push_back(options::option_t(options::names_t("--bind-cluster"),
                                             options::OPTIONAL_REPEAT,
                                             obsolescence::UNUSED_IGNORED));
    options_out->push_back(options::option_t(options::names_t("--bind-driver"),
                                             options::OPTIONAL_REPEAT));
    options_out->push_back(options::option_t(options::names_t("--bind-http"),
                                             options::OPTIONAL_REPEAT));
    help.add("--bind {all | addr}", "add the address of a local interface to listen on when accepting connections, loopback addresses are enabled by default. Can be overridden by the following three options.");
    help.add("--bind-cluster {all | addr}", "unused in ReFound.  override the behavior specified by --bind for cluster connections.");
    help.add("--bind-driver {all | addr}", "override the behavior specified by --bind for client driver connections.");
    help.add("--bind-http {all | addr}", "override the behavior specified by --bind for web console connections.");
    options_out->push_back(options::option_t(options::names_t("--no-default-bind"),
                                             options::OPTIONAL_NO_PARAMETER));
    help.add("--no-default-bind", "disable automatic listening on loopback addresses");

    options_out->push_back(options::option_t(options::names_t("--cluster-port"),
                                             options::OPTIONAL,
                                             strprintf("%d", port_defaults::peer_port.value),
                                             obsolescence::UNUSED_IGNORED));
    help.add("--cluster-port port", "unused in ReFound.  port for receiving connections from other servers");

    options_out->push_back(options::option_t(options::names_t("--client-port"),
                                             options::OPTIONAL,
                                             strprintf("%d", port_defaults::client_port.value),
                                             obsolescence::OBSOLETE_DISALLOWED));
#ifndef NDEBUG
    help.add("--client-port port", "obsolete and disallowed in ReFound.  port to use when connecting to other servers (for development)");
#endif  // NDEBUG

    options_out->push_back(options::option_t(options::names_t("--driver-port"),
                                             options::OPTIONAL,
                                             strprintf("%d", port_defaults::reql_port.value)));
    help.add("--driver-port port", "port for rethinkdb protocol client drivers");

    options_out->push_back(options::option_t(options::names_t("--port-offset", "-o"),
                                             options::OPTIONAL,
                                             strprintf("%d", port_defaults::port_offset)));
    help.add("-o [ --port-offset ] offset", "all ports used locally will have this value added");

    options_out->push_back(options::option_t(options::names_t("--join", "-j"),
                                             join_required ? options::MANDATORY_REPEAT : options::OPTIONAL_REPEAT,
                                             obsolescence::OBSOLETE_DISALLOWED));
    help.add("-j [ --join ] host[:port]", "obsolete and disallowed in ReFound.  Clusters are now "
        "joined by connecting to FoundationDB.");

    options_out->push_back(options::option_t(options::names_t("--reql-http-proxy"),
                                             options::OPTIONAL));
    help.add("--reql-http-proxy [protocol://]host[:port]", "HTTP proxy to use for performing `r.http(...)` queries, default port is 1080");

    options_out->push_back(options::option_t(options::names_t("--canonical-address"),
                                             options::OPTIONAL_REPEAT,
                                             obsolescence::UNUSED_IGNORED));
    help.add("--canonical-address host[:port]", "unused in ReFound.  address that other rethinkdb instances will use to connect to us, can be specified multiple times");

    options_out->push_back(options::option_t(options::names_t("--join-delay"),
                                             options::OPTIONAL,
                                             obsolescence::OBSOLETE_IGNORED));
    help.add("--join-delay seconds", "obsolete in ReFound.  hold the TCP connection open for these many "
             "seconds before joining with another server");

    options_out->push_back(options::option_t(options::names_t("--cluster-reconnect-timeout"),
                                             options::OPTIONAL,
                                             strprintf("%d", cluster_defaults::reconnect_timeout),
                                             obsolescence::OBSOLETE_IGNORED));
    help.add("--cluster-reconnect-timeout seconds", "obsolete in ReFound.  maximum number of seconds to "
                                                    "attempt reconnecting to a server "
                                                    "before giving up, the default is "
                                                    "24 hours");

    return help;
}

options::help_section_t get_cpu_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("CPU options");
    options_out->push_back(options::option_t(options::names_t("--cores", "-c"),
                                             options::OPTIONAL,
                                             strprintf("%d", get_cpu_count())));
    help.add("-c [ --cores ] n", "the number of cores to use");
    return help;
}

MUST_USE bool parse_cores_option(const std::map<std::string, options::values_t> &opts,
                                 int *num_workers_out) {
    int num_workers = get_single_int(opts, "--cores");
    const int max_cores = MAX_CORES;
    if (num_workers <= 0 || num_workers > max_cores) {
        fprintf(stderr, "ERROR: number specified for cores to use must be between 1 and %d\n", max_cores);
        return false;
    }
    *num_workers_out = num_workers;
    return true;
}

options::help_section_t get_service_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Service options");
    options_out->push_back(options::option_t(options::names_t("--pid-file"),
                                             options::OPTIONAL));
    help.add("--pid-file path", "a file in which to write the process id when the process is running");
    options_out->push_back(options::option_t(options::names_t("--daemon"),
                                             options::OPTIONAL_NO_PARAMETER));
    help.add("--daemon", "daemonize this rethinkdb process");
    return help;
}

options::help_section_t get_setuser_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Set User/Group options");
#ifndef _WIN32
    options_out->push_back(options::option_t(options::names_t("--runuser"),
                                             options::OPTIONAL));
    help.add("--runuser user", "run as the specified user");
    options_out->push_back(options::option_t(options::names_t("--rungroup"),
                                             options::OPTIONAL));
    help.add("--rungroup group", "run with the specified group");
#endif
    return help;
}

#ifdef ENABLE_TLS
options::help_section_t get_tls_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("TLS options");

    // Web TLS options.
    options_out->push_back(options::option_t(options::names_t("--http-tls-key"),
                                             options::OPTIONAL));
    options_out->push_back(options::option_t(options::names_t("--http-tls-cert"),
                                             options::OPTIONAL));
    help.add(
        "--http-tls-key key_filename",
        "private key to use for web administration console TLS");
    help.add(
        "--http-tls-cert cert_filename",
        "certificate to use for web administration console TLS");

    // Client Driver TLS options.
    options_out->push_back(options::option_t(options::names_t("--driver-tls-key"),
                                             options::OPTIONAL));
    options_out->push_back(options::option_t(options::names_t("--driver-tls-cert"),
                                             options::OPTIONAL));
    options_out->push_back(options::option_t(options::names_t("--driver-tls-ca"),
                                             options::OPTIONAL));
    help.add(
        "--driver-tls-key key_filename",
        "private key to use for client driver connection TLS");
    help.add(
        "--driver-tls-cert cert_filename",
        "certificate to use for client driver connection TLS");
    help.add(
        "--driver-tls-ca ca_filename",
        "CA certificate bundle used to verify client certificates; TLS client authentication disabled if omitted");

    // Client Driver TLS options.
    options_out->push_back(options::option_t(options::names_t("--cluster-tls-key"),
                                             options::OPTIONAL,
                                             obsolescence::UNUSED_IGNORED));
    options_out->push_back(options::option_t(options::names_t("--cluster-tls-cert"),
                                             options::OPTIONAL,
                                             obsolescence::UNUSED_IGNORED));
    options_out->push_back(options::option_t(options::names_t("--cluster-tls-ca"),
                                             options::OPTIONAL,
                                             obsolescence::UNUSED_IGNORED));
    help.add(
        "--cluster-tls-key key_filename",
        "unused in ReFound.  private key to use for intra-cluster connection TLS");
    help.add(
        "--cluster-tls-cert cert_filename",
        "unused in ReFound.  certificate to use for intra-cluster connection TLS");
    help.add(
        "--cluster-tls-ca ca_filename",
        "unused in ReFound.  CA certificate bundle used to verify cluster peer certificates");

    // Generic TLS options, for customizing the supported protocols and cipher suites.
    options_out->push_back(options::option_t(options::names_t("--tls-min-protocol"),
                                             options::OPTIONAL));
    options_out->push_back(options::option_t(options::names_t("--tls-ciphers"),
                                             options::OPTIONAL));
    options_out->push_back(options::option_t(options::names_t("--tls-ecdh-curve"),
                                             options::OPTIONAL));
    options_out->push_back(options::option_t(options::names_t("--tls-dhparams"),
                                             options::OPTIONAL));
    help.add(
        "--tls-min-protocol protocol",
        "the minimum TLS protocol version that the server accepts; options are "
        "'TLSv1', 'TLSv1.1', 'TLSv1.2'; default is 'TLSv1.2'");
    help.add(
        "--tls-ciphers cipher_list",
        "specify a list of TLS ciphers to use; default is 'EECDH+AESGCM'");
    help.add(
        "--tls-ecdh-curve curve_name",
        "specify a named elliptic curve to use for ECDHE; default is 'prime256v1'");
    help.add(
        "--tls-dhparams dhparams_filename",
        "provide parameters for DHE key agreement; REQUIRED if using DHE cipher suites; "
        "at least 2048-bit recommended");

    return help;
}
#endif

options::help_section_t get_help_options(std::vector<options::option_t> *options_out) {
    options::help_section_t help("Help options");
    options_out->push_back(options::option_t(options::names_t("--help", "-h"),
                                             options::OPTIONAL_NO_PARAMETER));
    options_out->push_back(options::option_t(options::names_t("--version", "-v"),
                                             options::OPTIONAL_NO_PARAMETER));
    help.add("-h [ --help ]", "print this help");
    help.add("-v [ --version ]", "print the version number of rethinkdb");
    return help;
}

// TODO: check all options for fdb (in all these options functions.

void get_rethinkdb_create_options(std::vector<options::help_section_t> *help_out,
                                  std::vector<options::option_t> *options_out) {
    help_out->push_back(get_fdb_options(options_out));
    help_out->push_back(get_fdb_create_options(options_out));
    help_out->push_back(get_file_options(options_out));
    help_out->push_back(get_server_options(options_out));
    help_out->push_back(get_auth_options(options_out));
    help_out->push_back(get_setuser_options(options_out));
    help_out->push_back(get_help_options(options_out));
    help_out->push_back(get_log_options(options_out));
    help_out->push_back(get_config_file_options(options_out));
}

void get_rethinkdb_serve_options(std::vector<options::help_section_t> *help_out,
                                 std::vector<options::option_t> *options_out) {
    help_out->push_back(get_fdb_options(options_out));
    help_out->push_back(get_file_options(options_out));
    help_out->push_back(get_network_options(false, options_out));
#ifdef ENABLE_TLS
    help_out->push_back(get_tls_options(options_out));
#endif
    help_out->push_back(get_auth_options(options_out));
    help_out->push_back(get_web_options(options_out));
    help_out->push_back(get_cpu_options(options_out));
    help_out->push_back(get_service_options(options_out));
    help_out->push_back(get_setuser_options(options_out));
    help_out->push_back(get_help_options(options_out));
    help_out->push_back(get_log_options(options_out));
    help_out->push_back(get_config_file_options(options_out));
}

// We no longer have a porcelain command, but to avoid excessive refactoring, we retain
// this function for its uses (such as configuration file validation).
void get_rethinkdb_porcelain_options(std::vector<options::help_section_t> *help_out,
                                     std::vector<options::option_t> *options_out) {
    help_out->push_back(get_fdb_options(options_out));
    help_out->push_back(get_fdb_create_options(options_out));
    help_out->push_back(get_file_options(options_out));
    help_out->push_back(get_server_options(options_out));
    help_out->push_back(get_network_options(false, options_out));
#ifdef ENABLE_TLS
    help_out->push_back(get_tls_options(options_out));
#endif
    help_out->push_back(get_auth_options(options_out));
    help_out->push_back(get_web_options(options_out));
    help_out->push_back(get_cpu_options(options_out));
    help_out->push_back(get_service_options(options_out));
    help_out->push_back(get_setuser_options(options_out));
    help_out->push_back(get_help_options(options_out));
    help_out->push_back(get_log_options(options_out));
    help_out->push_back(get_config_file_options(options_out));
}

std::map<std::string, options::values_t> parse_config_file_flat(const std::string &config_filepath,
                                                                const std::vector<options::option_t> &options) {
    std::string file;
    if (!blocking_read_file(config_filepath.c_str(), &file)) {
        throw std::runtime_error(strprintf("Trouble reading config file '%s'", config_filepath.c_str()));
    }

    std::vector<options::option_t> options_superset;
    std::vector<options::help_section_t> helps_superset;

    // There will be some duplicates in here, but it shouldn't be a problem
    get_rethinkdb_create_options(&helps_superset, &options_superset);
    get_rethinkdb_serve_options(&helps_superset, &options_superset);
    get_rethinkdb_porcelain_options(&helps_superset, &options_superset);

    return options::parse_config_file(file, config_filepath,
                                      options, options_superset);
}

std::map<std::string, options::values_t> parse_commands_deep(int argc, char **argv,
                                                             std::vector<options::option_t> options,
                                                             bool *fail_out) {
    std::map<std::string, options::values_t> opts = options::parse_command_line(argc, argv, options, fail_out);
    const optional<std::string> config_file_name = get_optional_option(opts, "--config-file");
    if (config_file_name.has_value()) {
        opts = options::merge(opts, parse_config_file_flat(*config_file_name, options));
    }
    opts = options::merge(opts, default_values_map(options));
    return opts;
}

void output_sourced_error(const options::option_error_t &ex) {
    fprintf(stderr, "Error in %s: %s\n", ex.source().c_str(), ex.what());
}

void output_named_error(const options::named_error_t &ex, const std::vector<options::help_section_t> &help) {
    output_sourced_error(ex);

    for (auto section = help.begin(); section != help.end(); ++section) {
        for (auto line = section->help_lines.begin(); line != section->help_lines.end(); ++line) {
            if (line->syntax_description.find(ex.option_name()) != std::string::npos) {
                std::vector<options::help_line_t> one_help_line;
                one_help_line.push_back(*line);
                std::vector<options::help_section_t> one_help_section;
                one_help_section.push_back(options::help_section_t("Usage", one_help_line));
                fprintf(stderr, "%s",
                        options::format_help(one_help_section).c_str());
                break;
            }
        }
    }
}

MUST_USE bool parse_io_threads_option(const std::map<std::string, options::values_t> &opts,
                                      int *max_concurrent_io_requests_out) {
    int max_concurrent_io_requests = get_single_int(opts, "--io-threads");
    if (max_concurrent_io_requests <= 0
        || max_concurrent_io_requests > MAXIMUM_MAX_CONCURRENT_IO_REQUESTS) {
        fprintf(stderr, "ERROR: io-threads must be between 1 and %d\n",
                MAXIMUM_MAX_CONCURRENT_IO_REQUESTS);
        return false;
    }
    *max_concurrent_io_requests_out = max_concurrent_io_requests;
    return true;
}

file_direct_io_mode_t parse_direct_io_mode_option(const std::map<std::string, options::values_t> &opts) {
    return exists_option(opts, "--direct-io") ?
        file_direct_io_mode_t::direct_desired :
        file_direct_io_mode_t::buffered_desired;
}

void append_littleendian_bit32(std::vector<uint8_t> *vec, uint64_t x) {
    // The "& 0xFF" expression is for clarity.
    vec->push_back(x & 0xFF);
    vec->push_back((x >> 8) & 0xFF);
    vec->push_back((x >> 16) & 0xFF);
    vec->push_back((x >> 24) & 0xFF);
}

void append_littleendian_bit64(std::vector<uint8_t> *vec, uint64_t x) {
    append_littleendian_bit32(vec, static_cast<uint32_t>(x));
    append_littleendian_bit32(vec, static_cast<uint32_t>(x >> 32));
}


std::vector<uint8_t> little_endian_uint64(uint64_t x) {
    // We're all using two's complement, right?
    std::vector<uint8_t> ret;
    ret.reserve(8);
    append_littleendian_bit64(&ret, x);
    return ret;
}
std::vector<uint8_t> little_endian_int64(int64_t x) {
    // We're all using two's complement, right?
    return little_endian_uint64(x);
}

std::string make_version_value(const fdb_cluster_id &cluster_id) {
    std::string version_value = REQLFDB_VERSION_VALUE_PREFIX;
    version_value += uuid_to_str(cluster_id.value);
    return version_value;
}

bool parse_reqlfdb_version_value_version_number(const uint8_t *value_data, size_t value_size, size_t *pos) {
    // We don't actually have any requirements on version number -- maybe 2.3.4a or
    // 2.3.5-f48a0cf48 are possible formats.
    size_t p = *pos;
    while (p < value_size && value_data[p] != ' ') {
        ++p;
    }
    if (p == *pos) {
        return false;
    }
    *pos = p;
    return true;
}

bool looks_like_reql_on_fdb_instance(const uint8_t *key_data, size_t key_size, const uint8_t *value_data, size_t value_size) {
    if (0 != sized_strcmp(key_data, key_size, as_uint8(REQLFDB_VERSION_KEY), strlen(REQLFDB_VERSION_KEY))) {
        return false;
    }

    size_t pos = strlen(REQLFDB_VERSION_VALUE_UNIVERSAL_PREFIX);
    // REQLFDB_VERSION_KEY is just the empty string, so we need to check the value
    if (!(pos <= value_size && 0 == memcmp(value_data, REQLFDB_VERSION_VALUE_UNIVERSAL_PREFIX, pos))) {
        return false;
    }

    // Now we parse the version number (but don't use it).
    std::string version_number;
    if (!parse_reqlfdb_version_value_version_number(value_data, value_size, &pos)) {
        return false;
    }

    if (!(pos < value_size && value_data[pos] == ' ')) {
        return false;
    }

    size_t nextpos = pos + uuid_u::kStringSize;
    uuid_u cluster_id;
    if (!(nextpos <= value_size && str_to_uuid(as_char(value_data + pos), uuid_u::kStringSize, &cluster_id))) {
        return false;
    }

    // That's as far as we parse it -- future versions might have more info afterwards.
    return true;
}

// Parses and returns cluster id.
optional<fdb_cluster_id> looks_like_usable_reql_on_fdb_instance(const uint8_t *key_data, size_t key_size, const uint8_t *value_data, size_t value_size) {
    if (0 != sized_strcmp(key_data, key_size, as_uint8(REQLFDB_VERSION_KEY), strlen(REQLFDB_VERSION_KEY))) {
        return r_nullopt;
    }

    size_t pos = strlen(REQLFDB_VERSION_VALUE_PREFIX);
    if (!(pos <= value_size && 0 == memcmp(value_data, REQLFDB_VERSION_VALUE_PREFIX, pos))) {
        return r_nullopt;
    }

    // That parses "reqlfdb 0.1.0 ".  Now for the cluster id.

    size_t nextpos = pos + uuid_u::kStringSize;
    uuid_u cluster_id;
    if (!(nextpos <= value_size && str_to_uuid(as_char(value_data + pos), uuid_u::kStringSize, &cluster_id))) {
        return r_nullopt;
    }

    // Since we're looking for our exact instance, we know there must be nothing past the cluster id.
    if (nextpos != value_size) {
        return r_nullopt;
    }

    // That's as far as we parse it -- future versions might have more info afterwards.
    return make_optional(fdb_cluster_id{cluster_id});
}

optional<fdb_cluster_id> main_rethinkdb_init_fdb_blocking_pthread(
        FDBDatabase *fdb, const bool wipe, const std::string &initial_password) {
    // Don't do fancy setup, just connect to FDB, check that it's empty (besides system
    // keys starting with \xFF), and then initialize the rethinkdb database.  TODO: Are
    // there fdb conventions for "claiming" your database?

    bool failure = false;

    std::vector<std::pair<FDBTransactionOption, std::vector<uint8_t>>> create_txn_options = {
        { FDB_TR_OPTION_TIMEOUT, little_endian_int64(5000) /* millis */ }
    };

    const fdb_cluster_id cluster_id{generate_uuid()};
    std::string cluster_id_str = uuid_to_str(cluster_id.value);
    logNTC("Connecting to FoundationDB to initialize RethinkDB instance %s...", cluster_id_str.c_str());

    std::string print_out;
    fdb_error_t loop_err = txn_retry_loop_pthread(fdb, create_txn_options,
        [&](FDBTransaction *txn) {
        logNTC("Attempting initialization of cluster %s...", cluster_id_str.c_str());
        printf_buffer_t print;
        // TODO: Prefix key option.

        uint8_t empty_key[1];
        int empty_key_length = 0;
        fdb_key_fut get_fut{fdb_transaction_get_key(
            txn,
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(empty_key, empty_key_length),
            false)};
        get_fut.block_pthread();

        key_view key;
        fdb_error_t err = future_get_key(get_fut.fut, &key);
        check_for_fdb_transaction(err);

        uint8_t end_key[1] = { 0xFF };
        int end_key_length = 1;

        std::string version_key = REQLFDB_VERSION_KEY;

        // Whether we have access to fdb system keys or not, "\xFF" or "\xFF..." is returned
        // for an empty database.
        if (sized_strcmp(key.data, key.length, end_key, end_key_length) < 0) {
            // If the database is non-empty, let's handle the case where it's populated by
            // a ReQL-on-FDB instance more informatively.

            fdb_future smallest_key_fut{fdb_transaction_get(txn, key.data, key.length, false)};
            smallest_key_fut.block_pthread();
            fdb_value value;
            fdb_error_t err = future_get_value(smallest_key_fut.fut, &value);
            check_for_fdb_transaction(err);
            guarantee(value.present);  // TODO: fdb, graceful -- fdb transaction semantics guarantees value exists

            if (looks_like_reql_on_fdb_instance(key.data, key.length, value.data, value.length)) {
                // Right, we don't use the parsed cluster_id here.
                print.appendf("Attempted RethinkDB initialization when existing instance exists.\n");
                print.appendf("RethinkDB instance identity: %.*s\n", value.length, as_char(value.data));
            } else {
                print.appendf("Attempted RethinkDB initialization on non-empty, unrecognized FoundationDB database.\n");
                print.appendf("Its first key (length %d) is: '%.*s'\n", key.length, key.length, as_char(key.data));
                print.appendf("Its first value (length %d) is: '%.*s'\n", value.length, value.length, as_char(value.data));
            }


            if (!wipe) {
                // TODO: Report error properly.
                print.appendf("Failed to initialize RethinkDB instance.\n");
                failure = true;
                print_out.append(print.data(), size_t(print.size()));
                return;
            } else {
                print.appendf("Wiping FoundationDB as requested.\n");
                // TODO: Test that key/value is valid reqlfdb version key/value.
                fdb_transaction_clear_range(txn,
                    empty_key, empty_key_length,
                    end_key, end_key_length);
            }
        }

        // Okay, now we have an empty db.

        {
            std::string version_value = make_version_value(cluster_id);

            fdb_transaction_set(txn,
                as_uint8(version_key.data()),
                version_key.size(),
                as_uint8(version_value.data()),
                version_value.size());
        }

        {
            const char *clock_key = REQLFDB_CLOCK_KEY;
            uint8_t value[REQLFDB_CLOCK_SIZE] = { 0 };
            fdb_transaction_set(txn,
                as_uint8(clock_key), strlen(clock_key),
                value, sizeof(value));
        }

        {
            const char *nodes_count_key = REQLFDB_NODES_COUNT_KEY;
            uint8_t value[REQLFDB_NODES_COUNT_SIZE] = { 0 };
            fdb_transaction_set(txn,
                as_uint8(nodes_count_key), strlen(nodes_count_key),
                value, sizeof(value));
        }

        {
            auth::username_t username("admin");
            // TODO: Is this duplicated code somewhere?
            // Use a single iteration for better efficiency when starting out with an
            // empty password.
            uint32_t iterations = initial_password.empty() ? 1 : auth::password_t::default_iteration_count;

            auth::user_t user(auth::password_t(initial_password, iterations));
            transaction_create_user(txn, username, user);
        }

        {
            name_string_t test_db_name = name_string_t::guarantee_valid("test");
            database_id_t test_db_id{generate_uuid()};

            transaction_create_db(txn, test_db_name, test_db_id);
        }

        {
            reqlfdb_config_version cv;
            cv.value = { 0 };
            transaction_set_config_version(txn, cv);
        }

        fdb_future commit_fut{fdb_transaction_commit(txn)};
        commit_fut.block_pthread();

        err = fdb_future_get_error(commit_fut.fut);
        check_for_fdb_transaction(err);

        failure = false;
        print_out.append(print.data(), size_t(print.size()));
        return;
    });
    if (loop_err != 0) {
        logERR("Error in FoundationDB transaction: %s\n", fdb_get_error(loop_err));
        return r_nullopt;
    }

    if (failure) {
        logERR("%s", print_out.c_str());
        return r_nullopt;
    } else {
        logNTC("%s", print_out.c_str());
        logNTC("Successfully initialized RethinkDB instance '%s' in FoundationDB\n",
            cluster_id_str.c_str());
        return make_optional(cluster_id);
    }
}

optional<fdb_cluster_id> main_rethinkdb_create_fdb_blocking_pthread(FDBDatabase *fdb,
    const std::string &initial_password) {
    // All we do is connect to FDB, read and parse the first key.

    cond_t non_interruptor;

    bool failure = false;

    std::vector<std::pair<FDBTransactionOption, std::vector<uint8_t>>> create_txn_options = {
        { FDB_TR_OPTION_TIMEOUT, little_endian_int64(5000) /* millis */ }
    };

    fdb_cluster_id cluster_id{nil_uuid()};

    logNTC("Connecting to FoundationDB to retrieve RethinkDB instance information...");

    bool initial_password_already_configured = false;

    std::string print_out;
    fdb_error_t loop_err = txn_retry_loop_pthread(fdb, create_txn_options,
        [&](FDBTransaction *txn) {
        printf_buffer_t print;

        static_assert(REQLFDB_KEY_PREFIX_NOT_IMPLEMENTED, "FDB key prefix assumed not implemented here.");
        uint8_t empty_key[1];
        int empty_key_length = 0;
        fdb_key_fut get_fut{fdb_transaction_get_key(
            txn,
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(empty_key, empty_key_length),
            false)};
        get_fut.block_pthread();

        key_view key;
        fdb_error_t err = future_get_key(get_fut.fut, &key);
        check_for_fdb_transaction(err);

        uint8_t end_key[1] = { 0xFF };
        int end_key_length = 1;

        std::string version_key = REQLFDB_VERSION_KEY;

        // Whether we have access to fdb system keys or not, "\xFF" or "\xFF..." is returned
        // for an empty database.
        if (sized_strcmp(key.data, key.length, end_key, end_key_length) < 0) {
            // If the database is non-empty, let's see what we've got.

            fdb_future smallest_key_fut{fdb_transaction_get(txn, key.data, key.length, false)};
            smallest_key_fut.block_pthread();
            fdb_value value;
            fdb_error_t err = future_get_value(smallest_key_fut.fut, &value);
            check_for_fdb_transaction(err);
            guarantee(value.present);  // TODO: fdb, graceful -- fdb transaction semantics guarantees value exists

            if (optional<fdb_cluster_id> cluster_id_opt = looks_like_usable_reql_on_fdb_instance(key.data, key.length, value.data, value.length)) {
                // Okay, we parsed and got the cluster id.
                cluster_id = *cluster_id_opt;
                failure = false;
            } else {
                print.appendf("Attempted RethinkDB creation while connecting to a non-empty, unrecognized FoundationDB database.\n");
                print.appendf("Its first key (length %d) is: '%.*s'\n", key.length, key.length, as_char(key.data));
                print.appendf("Its first value (length %d) is: '%.*s'\n", value.length, value.length, as_char(value.data));
                failure = true;
                print_out.append(print.data(), size_t(print.size()));
                return;
            }
        } else {
            print.appendf("Attempted RethinkDB creation (without initializing) while connecting to an empty FoundationDB database.\n");
            failure = true;
            print_out.append(print.data(), size_t(print.size()));
            return;
        }
        if (!initial_password.empty()) {
            bool mutated = set_initial_password(txn, &non_interruptor, initial_password);
            if (mutated) {
                // We have changes, so commit.
                commit(txn, &non_interruptor);
            }
            initial_password_already_configured = !mutated;
        }

        print_out.append(print.data(), size_t(print.size()));
        return;
    });
    if (loop_err != 0) {
        logERR("Error in FoundationDB transaction: %s\n", fdb_get_error(loop_err));
        return r_nullopt;
    }

    if (failure) {
        logERR("%s", print_out.c_str());
        return r_nullopt;
    } else {
        if (initial_password_already_configured) {
            logNTC(initial_password_msg);
        }
        logNTC("%s", print_out.c_str());
        return make_optional(cluster_id);
    }
}

int main_rethinkdb_create(int argc, char *argv[]) {
    std::vector<options::option_t> options;
    std::vector<options::help_section_t> help;
    get_rethinkdb_create_options(&help, &options);

    try {
        bool fail;
        std::map<std::string, options::values_t> opts
            = parse_commands_deep(argc, argv, options, &fail);
        if (fail) {
            return EXIT_FAILURE;
        }

        // TODO: Copy/paste boilerplate.
        if (handle_help_or_version_option(opts, &help_rethinkdb_create)) {
            return EXIT_SUCCESS;
        }

        options::verify_option_counts(options, opts);

        bool init, wipe;
        if (!parse_init_options(opts, &init, &wipe)) {
            return EXIT_FAILURE;
        }

        base_path_t base_path(get_single_option(opts, "--directory"));

        name_string_t server_name = parse_server_name_option(opts);
        std::set<name_string_t> server_tag_names = parse_server_tag_options(opts);
        std::string initial_password = parse_initial_password_option(opts);
        optional<uint64_t> total_cache_size;
        if (optional<optional<uint64_t> > x =
                parse_total_cache_size_option(opts)) {
            total_cache_size = *x;
        }

        int max_concurrent_io_requests;
        if (!parse_io_threads_option(opts, &max_concurrent_io_requests)) {
            return EXIT_FAILURE;
        }

        bool is_new_directory = false;
        directory_lock_t data_directory_lock(base_path, true, &is_new_directory);

        /* This is redundant with check_dir_emptiness below, but distinguishes between
           having an existing node's data directory and a nonsensically initialized data
           directory, for better error output. */
        if (optional<fdb_cluster_id> cluster_id = read_cluster_id_file(base_path)) {
            fprintf(stderr,
                "The data directory '%s' is already used for the cluster '%s'.\n"
                "Delete the directory or use another location.\n",
                base_path.path().c_str(),
                uuid_to_str(cluster_id->value).c_str());
            return EXIT_FAILURE;
        }

        if (!check_dir_emptiness(base_path, {LOG_FILE_DEFAULT_FILENAME, FDB_CLUSTER_DEFAULT_FILENAME})) {
            fprintf(stderr,
                "The data directory '%s', if it already exists, must be\n"
                "empty upon creation, except for the files '%s' and '%s'.\n"
                "Clean up the directory or use another location.\n",
                base_path.path().c_str(), LOG_FILE_DEFAULT_FILENAME, FDB_CLUSTER_DEFAULT_FILENAME);
            return EXIT_FAILURE;
        }

#ifndef _WIN32
        get_and_set_user_group_and_directory(opts, &data_directory_lock);
#endif

        // NNN: Did we not use make_absolute for base_path in the create case before?
        // This is an inconsistency.
        initialize_logfile(opts, base_path);

        std::string fdb_cluster_file_param = get_fdb_client_cluster_file_param(base_path, opts);

        // QQQ: For fdb, remove the direct_io mode options.
        // const file_direct_io_mode_t direct_io_mode = parse_direct_io_mode_option(opts);

        fdb_startup_shutdown fdb_startup_shutdown;
        fdb_database fdb(fdb_cluster_file_param.c_str());

        optional<fdb_cluster_id> result2;
        if (init) {
            result2 = main_rethinkdb_init_fdb_blocking_pthread(
                fdb.db, wipe, initial_password);
        } else {
            result2 = main_rethinkdb_create_fdb_blocking_pthread(
                fdb.db, initial_password);
        }

        if (result2.has_value()) {
            // We'll initialize our node_id file first to make stronger our expectation
            // that cluster_id file existence implies node_id cluster file existence.
            fdb_node_id our_node_id{generate_uuid()};
            initialize_node_id_file(base_path, our_node_id);

            initialize_cluster_id_file(base_path, *result2);

            // Tell the directory lock that the directory is now good to go, as it
            //  will otherwise delete an uninitialized directory
            data_directory_lock.directory_initialized();
            return EXIT_SUCCESS;
        }
    } catch (const options::named_error_t &ex) {
        output_named_error(ex, help);
        fprintf(stderr, "Run 'rethinkdb help create' for help on the command\n");
    } catch (const options::option_error_t &ex) {
        output_sourced_error(ex);
        fprintf(stderr, "Run 'rethinkdb help create' for help on the command\n");
    } catch (const std::exception &ex) {
        fprintf(stderr, "%s\n", ex.what());
    }
    return EXIT_FAILURE;
}

bool maybe_daemonize(const std::map<std::string, options::values_t> &opts) {
    if (exists_option(opts, "--daemon")) {
#ifdef _WIN32
        // TODO WINDOWS
        fail_due_to_user_error("--daemon not implemented on windows");
#else
        pid_t pid = fork();
        if (pid < 0) {
            throw std::runtime_error(strprintf("Failed to fork daemon: %s\n", errno_string(get_errno()).c_str()).c_str());
        }

        if (pid > 0) {
            return false;
        }

        umask(0);

        pid_t sid = setsid();
        if (sid == 0) {
            throw std::runtime_error(strprintf("Failed to create daemon session: %s\n", errno_string(get_errno()).c_str()).c_str());
        }

        if (chdir("/") < 0) {
            throw std::runtime_error(strprintf("Failed to change directory: %s\n", errno_string(get_errno()).c_str()).c_str());
        }

        if (freopen("/dev/null", "r", stdin) == nullptr) {
            throw std::runtime_error(strprintf("Failed to redirect stdin for daemon: %s\n", errno_string(get_errno()).c_str()).c_str());
        }
        if (freopen("/dev/null", "w", stdout) == nullptr) {
            throw std::runtime_error(strprintf("Failed to redirect stdin for daemon: %s\n", errno_string(get_errno()).c_str()).c_str());
        }
        if (freopen("/dev/null", "w", stderr) == nullptr) {
            throw std::runtime_error(strprintf("Failed to redirect stderr for daemon: %s\n", errno_string(get_errno()).c_str()).c_str());
        }
#endif
    }
    return true;
}

int main_rethinkdb_serve(int argc, char *argv[]) {
    std::vector<options::option_t> options;
    std::vector<options::help_section_t> help;
    get_rethinkdb_serve_options(&help, &options);

    try {
        bool fail;
        std::map<std::string, options::values_t> opts = parse_commands_deep(argc, argv, options, &fail);
        if (fail) {
            return EXIT_FAILURE;
        }

        if (handle_help_or_version_option(opts, &help_rethinkdb_serve)) {
            return EXIT_SUCCESS;
        }

        options::verify_option_counts(options, opts);

#ifndef _WIN32
        get_and_set_user_group(opts);
#endif

        base_path_t base_path(get_single_option(opts, "--directory"));

        std::string initial_password = parse_initial_password_option(opts);

        UNUSED std::vector<host_and_port_t> joins = parse_join_options(opts, port_defaults::peer_port);

        service_address_ports_t address_ports = get_service_address_ports(opts);

        std::string web_path = get_web_path(opts);

        int num_workers;
        if (!parse_cores_option(opts, &num_workers)) {
            return EXIT_FAILURE;
        }

        // QQQ: Remove this option for fdb, it's unused.
        int max_concurrent_io_requests;
        if (!parse_io_threads_option(opts, &max_concurrent_io_requests)) {
            return EXIT_FAILURE;
        }

        // TODO: We don't use total_cache_size.
        UNUSED optional<optional<uint64_t> > total_cache_size =
            parse_total_cache_size_option(opts);

        UNUSED optional<int> join_delay_secs = parse_join_delay_secs_option(opts);
        optional<int> node_reconnect_timeout_secs =
            parse_node_reconnect_timeout_secs_option(opts);

        // Open and lock the directory, but do not create it
        bool is_new_directory = false;
        directory_lock_t data_directory_lock(base_path, false, &is_new_directory);

        guarantee(!is_new_directory);

        optional<fdb_cluster_id> cluster_id = read_cluster_id_file(base_path);
        if (!cluster_id.has_value()) {
            fprintf(stderr,
                "The data directory '%s' is missing its cluster_id file.\n"
                "You need to use 'rethinkdb create' to initialize the directory.\n",
                base_path.path().c_str());
            return EXIT_FAILURE;
        }

        fdb_node_id node_id = read_node_id_file(base_path);

        base_path = base_path.make_absolute();
        initialize_logfile(opts, base_path);

        std::string fdb_cluster_file_param = get_fdb_client_cluster_file_param(base_path, opts);

        if (check_pid_file(opts) != EXIT_SUCCESS) {
            return EXIT_FAILURE;
        }

        if (!maybe_daemonize(opts)) {
            // This is the parent process of the daemon, just exit
            return EXIT_SUCCESS;
        }

        if (write_pid_file(opts) != EXIT_SUCCESS) {
            return EXIT_FAILURE;
        }

        extproc_spawner_t extproc_spawner;

        tls_configs_t tls_configs;
#ifdef ENABLE_TLS
        if (!configure_tls(opts, &tls_configs)) {
            return EXIT_FAILURE;
        }
#endif

        serve_info_t serve_info(*cluster_id,
                                node_id,
                                get_reql_http_proxy_option(opts),
                                std::move(web_path),
                                address_ports,
                                get_optional_option(opts, "--config-file"),
                                std::vector<std::string>(argv, argv + argc),
                                node_reconnect_timeout_secs.value_or(cluster_defaults::reconnect_timeout),
                                tls_configs);

        // QQQ: Remove this option and such for fdb.
        // const file_direct_io_mode_t direct_io_mode = parse_direct_io_mode_option(opts);

        fdb_startup_shutdown fdb_startup_shutdown;
        fdb_database fdb(fdb_cluster_file_param.c_str());

        bool result;
        run_in_thread_pool(std::bind(&run_rethinkdb_serve,
                                     fdb.db,
                                     base_path,
                                     &serve_info,
                                     initial_password,
                                     &data_directory_lock,
                                     &result),
                           num_workers);
        return result ? EXIT_SUCCESS : EXIT_FAILURE;
    } catch (const options::named_error_t &ex) {
        output_named_error(ex, help);
        fprintf(stderr, "Run 'rethinkdb help serve' for help on the command\n");
    } catch (const options::option_error_t &ex) {
        output_sourced_error(ex);
        fprintf(stderr, "Run 'rethinkdb help serve' for help on the command\n");
    } catch (const std::exception& ex) {
        fprintf(stderr, "%s\n", ex.what());
    }
    return EXIT_FAILURE;
}

MUST_USE bool split_db_table(const std::string &db_table, std::string *db_name_out, std::string *table_name_out) {
    size_t first_pos = db_table.find_first_of('.');
    if (first_pos == std::string::npos || db_table.find_last_of('.') != first_pos) {
        return false;
    }

    if (first_pos == 0 || first_pos + 1 == db_table.size()) {
        return false;
    }

    db_name_out->assign(db_table.data(), first_pos);
    table_name_out->assign(db_table.data() + first_pos + 1, db_table.data() + db_table.size());
    guarantee(db_name_out->size() > 0);
    guarantee(table_name_out->size() > 0);
    return true;
}

void run_backup_script(const std::string& script_name, char * const arguments[]) {
    int res = execvp(script_name.c_str(), arguments);
    if (res == -1) {

        fprintf(stderr, "Error when launching '%s': %s\n",
                script_name.c_str(), errno_string(get_errno()).c_str());
        fprintf(stderr, "The %s command depends on the RethinkDB Python driver, which must be installed.\n",
                script_name.c_str());
        fprintf(stderr, "If the Python driver is already installed, make sure that the PATH environment variable\n"
                "includes the location of the backup scripts, and that the current user has permission to\n"
                "access and run the scripts.\n"
                "Instructions for installing the RethinkDB Python driver are available here:\n"
                "http://www.rethinkdb.com/docs/install-drivers/python/\n");
    }
}

int main_rethinkdb_export(int, char *argv[]) {
    run_backup_script(RETHINKDB_EXPORT_SCRIPT, argv + 1);
    return EXIT_FAILURE;
}

int main_rethinkdb_import(int, char *argv[]) {
    run_backup_script(RETHINKDB_IMPORT_SCRIPT, argv + 1);
    return EXIT_FAILURE;
}

int main_rethinkdb_dump(int, char *argv[]) {
    run_backup_script(RETHINKDB_DUMP_SCRIPT, argv + 1);
    return EXIT_FAILURE;
}

int main_rethinkdb_restore(int, char *argv[]) {
    run_backup_script(RETHINKDB_RESTORE_SCRIPT, argv + 1);
    return EXIT_FAILURE;
}

int main_rethinkdb_index_rebuild(int, char *argv[]) {
    run_backup_script(RETHINKDB_INDEX_REBUILD_SCRIPT, argv + 1);
    return EXIT_FAILURE;
}

int main_rethinkdb_repl(int, char *argv[]) {
    run_backup_script(RETHINKDB_REPL_SCRIPT, argv + 1);
    return EXIT_FAILURE;
}

void help_rethinkdb_porcelain() {
    // TODO: Update help message for fdb (there is no porcelain command now, too).
    std::vector<options::help_section_t> help_sections;
    {
        std::vector<options::option_t> options;
        get_rethinkdb_porcelain_options(&help_sections, &options);
    }

    printf(
        "Running 'rethinkdb' without a command is the same as running 'rethinkdb serve'.\n"
        "You must now, in ReFound, manually create a new metadata directory (or adopt a\n"
        "prepared directory) using 'rethinkdb create'.\n");
    printf("%s", format_help(help_sections).c_str());
    printf("\n");
    printf("There are a number of subcommands for more specific tasks:\n");
    printf("    'rethinkdb create': prepare files on disk for a new server instance\n");
    printf("    'rethinkdb serve': use an existing data directory to host data and serve queries\n");
    printf("    'rethinkdb proxy': not supported in ReFound.  Use 'rethinkdb serve'.\n");
    printf("    'rethinkdb export': export data from an existing cluster into a file or directory\n");
    printf("    'rethinkdb import': import data from from a file or directory into an existing cluster\n");
    printf("    'rethinkdb dump': export and compress data from an existing cluster\n");
    printf("    'rethinkdb restore': import compressed data into an existing cluster\n");
    printf("    'rethinkdb index-rebuild': rebuild outdated secondary indexes\n");
    printf("    'rethinkdb repl': start a Python REPL with the RethinkDB driver\n");
#ifdef _WIN32
    printf("    'rethinkdb install-service': install RethinkDB as a Windows service\n");
    printf("    'rethinkdb remove-service': remove a previously installed Windows service\n");
#endif
    printf("\n");
    printf("For more information, run 'rethinkdb help [subcommand]'.\n");
}

void help_rethinkdb_create() {
    std::vector<options::help_section_t> help_sections;
    {
        std::vector<options::option_t> options;
        get_rethinkdb_create_options(&help_sections, &options);
    }

    printf(
        "'rethinkdb create' is used to prepare a directory to act as the\n"
        "storage location for a RethinkDB server.  The directory may\n"
        "pre-exist and contain an 'fdb.cluster' file.\n");
    printf("%s", format_help(help_sections).c_str());
}

void help_rethinkdb_serve() {
    std::vector<options::help_section_t> help_sections;
    {
        std::vector<options::option_t> options;
        get_rethinkdb_serve_options(&help_sections, &options);
    }

    printf("'rethinkdb serve' is the actual process for a RethinkDB server.\n");
    printf("%s", format_help(help_sections).c_str());
}

void help_rethinkdb_proxy() {
    printf("'rethinkdb proxy' is not supported in ReFound.  Now every node is a proxy.\n");
}

void help_rethinkdb_export() {
    char help_arg[] = "--help";
    char dummy_arg[] = RETHINKDB_EXPORT_SCRIPT;
    char* args[3] = { dummy_arg, help_arg, nullptr };
    run_backup_script(RETHINKDB_EXPORT_SCRIPT, args);
}

void help_rethinkdb_import() {
    char help_arg[] = "--help";
    char dummy_arg[] = RETHINKDB_IMPORT_SCRIPT;
    char* args[3] = { dummy_arg, help_arg, nullptr };
    run_backup_script(RETHINKDB_IMPORT_SCRIPT, args);
}

void help_rethinkdb_dump() {
    char help_arg[] = "--help";
    char dummy_arg[] = RETHINKDB_DUMP_SCRIPT;
    char* args[3] = { dummy_arg, help_arg, nullptr };
    run_backup_script(RETHINKDB_DUMP_SCRIPT, args);
}

void help_rethinkdb_restore() {
    char help_arg[] = "--help";
    char dummy_arg[] = RETHINKDB_RESTORE_SCRIPT;
    char* args[3] = { dummy_arg, help_arg, nullptr };
    run_backup_script(RETHINKDB_RESTORE_SCRIPT, args);
}

void help_rethinkdb_index_rebuild() {
    char help_arg[] = "--help";
    char dummy_arg[] = RETHINKDB_INDEX_REBUILD_SCRIPT;
    char* args[3] = { dummy_arg, help_arg, nullptr };
    run_backup_script(RETHINKDB_INDEX_REBUILD_SCRIPT, args);
}

void help_rethinkdb_repl() {
    char help_arg[] = "--help";
    char dummy_arg[] = RETHINKDB_REPL_SCRIPT;
    char* args[3] = { dummy_arg, help_arg, nullptr };
    run_backup_script(RETHINKDB_REPL_SCRIPT, args);
}

#ifdef _WIN32

int global_windows_service_argc;
scoped_array_t<char *> global_windows_service_argv;
int main_rethinkdb_run_service(int argc, char *argv[]) {
    // Open stdin, stdout, stderr. We ignore errors here. If stderr or stdout fail to
    // open, the log writer is going to raise an issue later.
    freopen("NUL", "r", stdin);
    freopen("NUL", "w", stdout);
    freopen("NUL", "w", stderr);

    // We need to get our actual arguments into the main function callback.
    // StartServiceCtrlDispatcher requires a C function pointer, so we can't
    // use a C++ lambda with a clojure. Instead we preserve those values in
    // a pair of global variables.
    // We skip the second argument, which is just `run-service`.
    guarantee(argc >= 2);
    guarantee(strcmp(argv[1], "run-service") == 0);
    global_windows_service_argc = argc - 1;
    global_windows_service_argv.init(argc - 1);
    int out_i = 0;
    for (int i = 0; i < argc; ++i) {
        if (i == 1) {
            continue;
        }
        global_windows_service_argv[out_i] = argv[i];
        ++out_i;
    }

    SERVICE_TABLE_ENTRY dispatch_table[] = {
        { "", [](DWORD, char **) {
                  // Note that the arguments passed to this function are
                  // not the arguments specified when installing the service.
                  // Instead these are arguments that can be specified ad-hoc
                  // when *starting* the service. We don't particularly care
                  // about those, and ignore them here.
                  return windows_service_main_function(
                      main_rethinkdb_porcelain,
                      global_windows_service_argc,
                      global_windows_service_argv.data());
              } },
        { nullptr, nullptr }
    };

    if (!StartServiceCtrlDispatcher(dispatch_table)) {
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

void get_rethinkdb_install_service_options(
    std::vector<options::help_section_t> *help_out,
    std::vector<options::option_t> *options_out) {
    options::help_section_t help("Service options");
    options_out->push_back(options::option_t(options::names_t("--instance-name"),
        options::OPTIONAL));
    help.add("--instance-name name", "name under which the service will be installed. "
        "Specify this if you want to run multiple instances "
        "of RethinkDB on this server (default: \"default\")");
    options_out->push_back(options::option_t(options::names_t("--runuser"),
        options::OPTIONAL));
    help.add("--runuser user", "run the service under the given user account, "
        "specified in the format DomainName\\UserName. "
        "If not specified, the LocalSystem account is used");
    options_out->push_back(options::option_t(options::names_t("--runuser-password"),
        options::OPTIONAL));
    help.add("--runuser-password password", "password of the user specified in "
        "the `--runuser` option");
    help_out->push_back(help);
    help_out->push_back(get_config_file_options(options_out));
    help_out->push_back(get_help_options(options_out));
}

void help_rethinkdb_install_service() {
    std::vector<options::help_section_t> help_sections;
    {
        std::vector<options::option_t> options;
        get_rethinkdb_install_service_options(&help_sections, &options);
    }

    printf("'rethinkdb install-service' installs RethinkDB as a Windows service.\n");
    printf("%s", format_help(help_sections).c_str());
}

void get_rethinkdb_remove_service_options(
    std::vector<options::help_section_t> *help_out,
    std::vector<options::option_t> *options_out) {
    options::help_section_t help("Service options");
    options_out->push_back(options::option_t(options::names_t("--instance-name"),
        options::OPTIONAL));
    help.add("--instance-name name", "name of the instance that will be removed. "
        "The name must match the one used when installing the service "
        "(default: \"default\")");
    help_out->push_back(help);
    help_out->push_back(get_help_options(options_out));
}

void help_rethinkdb_remove_service() {
    std::vector<options::help_section_t> help_sections;
    {
        std::vector<options::option_t> options;
        get_rethinkdb_remove_service_options(&help_sections, &options);
    }

    printf("'rethinkdb remove-service' removes a previously installed Windows service.\n");
    printf("%s", format_help(help_sections).c_str());
}

const char *ELEVATED_RUN_MARKER = "_rethinkdb_was_elevated";

bool was_elevated(int argc, char *argv[]) {
    return argc > 0 && strcmp(argv[argc - 1], ELEVATED_RUN_MARKER) == 0;
}

int restart_elevated(int argc, char *argv[]) {
    SHELLEXECUTEINFO sei;
    memset(&sei, 0, sizeof(sei));
    sei.cbSize = sizeof(sei);
    sei.lpVerb = "runas";
    sei.fMask = SEE_MASK_NOCLOSEPROCESS;
    sei.nShow = SW_NORMAL;
    sei.lpFile = argv[0];
    std::string args;
    for (int i = 1; i < argc; ++i) {
        if (i > 1) {
            args += " ";
        }
        args += escape_windows_shell_arg(argv[i]);
    }
    // Mark as elevated
    args += " ";
    args += ELEVATED_RUN_MARKER;
    sei.lpParameters = args.c_str();

    if (!ShellExecuteEx(&sei)) {
        DWORD err = GetLastError();
        if (err == ERROR_CANCELLED) {
            fprintf(stderr, "The request to elevate permissions was rejected.\n");
        } else {
            fprintf(stderr, "ShellExecuteEx failed: %s\n", winerr_string(err).c_str());
        }
        return EXIT_FAILURE;
    } else {
        if (sei.hProcess != nullptr) {
            WaitForSingleObject(sei.hProcess, INFINITE);
            DWORD exit_code;
            if (GetExitCodeProcess(sei.hProcess, &exit_code)) {
                CloseHandle(sei.hProcess);
                return exit_code;
            }
            CloseHandle(sei.hProcess);
        }
        fprintf(stderr, "Could not get a handler on the child process.\n");
        return EXIT_FAILURE;
    }
}

int run_and_maybe_elevate(
        bool already_elevated,
        int argc,
        char *argv[],
        const std::function<bool()> &f) {
    try {
        bool success = f();
        if (already_elevated) {
            // Sleep a bit so that any output from the elevated shell window remains visible.
            Sleep(5000);
        }
        return success ? EXIT_SUCCESS : EXIT_FAILURE;
    } catch (const windows_privilege_exc_t &) {
        if (!already_elevated) {
            fprintf(stderr, "Permission denied. Trying again with elevated permissions...\n");
            return restart_elevated(argc, argv);
        } else {
            fprintf(stderr, "Permission denied\n");
            return EXIT_FAILURE;
        }
    }
}

int main_rethinkdb_install_service(int argc, char *argv[]) {
    bool already_elevated = was_elevated(argc, argv);
    if (already_elevated) {
        --argc;
    }

    std::vector<options::option_t> options;
    std::vector<options::help_section_t> help;
    get_rethinkdb_install_service_options(&help, &options);
    try {
        std::map<std::string, options::values_t> opts = parse_command_line(argc - 2, argv + 2, options);

        if (handle_help_or_version_option(opts, &help_rethinkdb_install_service)) {
            return EXIT_SUCCESS;
        }

        options::verify_option_counts(options, opts);

        const optional<std::string> config_file_name_arg =
            get_optional_option(opts, "--config-file");
        if (!config_file_name_arg) {
            fprintf(stderr, "rethinkdb install-service requires the `--config-file` option.\n");
            fprintf(stderr,
                "You can find a template for the configuration file at "
                "<https://github.com/rethinkdb/rethinkdb/blob/next/packaging/assets/config/default.conf.sample>.\n");
            return EXIT_FAILURE;
        }
        // Make the config file name absolute
        TCHAR full_path[MAX_PATH];
        DWORD full_path_length = GetFullPathName(
            config_file_name_arg->c_str(),
            MAX_PATH,
            full_path,
            nullptr);
        if (full_path_length >= MAX_PATH) {
            fprintf(
                stderr,
                "The absolute path to the configuration file is too long. "
                "It must be shorter than %zu.\n",
                static_cast<size_t>(MAX_PATH));
            return EXIT_FAILURE;
        } else if (full_path_length == 0) {
            fprintf(
                stderr,
                "Failed to convert the configuration file path to an absolute path: %s\n",
                winerr_string(GetLastError()).c_str());
            return EXIT_FAILURE;
        }
        std::string config_file_name(full_path, full_path_length);

        // Validate the configuration file (we ignore the resulting options)
        {
            std::vector<options::help_section_t> help;
            std::vector<options::option_t> valid_options;
            get_rethinkdb_porcelain_options(&help, &valid_options);
            parse_config_file_flat(config_file_name, valid_options);
        }

        const char *runuser_ptr = nullptr;
        const optional<std::string> runuser = get_optional_option(opts, "--runuser");
        if (runuser) {
            runuser_ptr = runuser->c_str();
        }
        const char *runuser_password_ptr = nullptr;
        const optional<std::string> runuser_password
            = get_optional_option(opts, "--runuser-password");
        if (runuser_password) {
            runuser_password_ptr = runuser_password->c_str();
        }

        std::string instance_name = "default";
        const optional<std::string> instance_name_arg =
            get_optional_option(opts, "--instance-name");
        if (instance_name_arg) {
            instance_name = *instance_name_arg;
        }

        // Get our filename
        TCHAR my_path[MAX_PATH];
        if (!GetModuleFileName(nullptr, my_path, MAX_PATH)) {
            fprintf(stderr, "Unable to retrieve own path: %s\n",
                winerr_string(GetLastError()).c_str());
            return EXIT_FAILURE;
        }

        std::string service_name = "rethinkdb_" + instance_name;
        std::string display_name = "RethinkDB (" + instance_name + ")";

        std::vector<std::string> service_args;
        service_args.push_back("run-service");
        service_args.push_back("--config-file");
        service_args.push_back(config_file_name);

        return run_and_maybe_elevate(already_elevated, argc, argv, [&]() {
            fprintf(stderr, "Installing service `%s`...\n", service_name.c_str());
            bool success = install_windows_service(
                service_name, display_name, std::string(my_path), service_args,
                runuser_ptr, runuser_password_ptr);
            if (success) {
                fprintf(stderr, "Service `%s` installed.\n", service_name.c_str());
            }
            fprintf(stderr, "Starting service `%s`...\n", service_name.c_str());
            if (start_windows_service(service_name)) {
                fprintf(stderr, "Service `%s` started.\n", service_name.c_str());
            }
            return success;
        });
    } catch (const options::named_error_t &ex) {
        output_named_error(ex, help);
        fprintf(stderr, "Run 'rethinkdb help install-service' for help on the command\n");
    } catch (const options::option_error_t &ex) {
        output_sourced_error(ex);
        fprintf(stderr, "Run 'rethinkdb help install-service' for help on the command\n");
    } catch (const std::exception& ex) {
        fprintf(stderr, "%s\n", ex.what());
    }
    return EXIT_FAILURE;
}

int main_rethinkdb_remove_service(int argc, char *argv[]) {
    bool already_elevated = was_elevated(argc, argv);
    if (already_elevated) {
        --argc;
    }

    std::vector<options::option_t> options;
    std::vector<options::help_section_t> help;
    get_rethinkdb_remove_service_options(&help, &options);
    try {
        std::map<std::string, options::values_t> opts = parse_command_line(argc - 2, argv + 2, options);

        if (handle_help_or_version_option(opts, &help_rethinkdb_remove_service)) {
            return EXIT_SUCCESS;
        }

        options::verify_option_counts(options, opts);

        std::string instance_name = "default";
        const optional<std::string> instance_name_arg =
            get_optional_option(opts, "--instance-name");
        if (instance_name_arg) {
            instance_name = *instance_name_arg;
        }

        std::string service_name = "rethinkdb_" + instance_name;

        return run_and_maybe_elevate(already_elevated, argc, argv, [&]() {
            fprintf(stderr, "Stopping service `%s`...\n", service_name.c_str());
            if (stop_windows_service(service_name)) {
                fprintf(stderr, "Service `%s` stopped.\n", service_name.c_str());
            }
            fprintf(stderr, "Removing service `%s`...\n", service_name.c_str());
            bool success = remove_windows_service(service_name);
            if (success) {
                fprintf(stderr, "Service `%s` removed.\n", service_name.c_str());
            }
            return success;
        });
    } catch (const options::named_error_t &ex) {
        output_named_error(ex, help);
        fprintf(stderr, "Run 'rethinkdb help remove-service' for help on the command\n");
    } catch (const options::option_error_t &ex) {
        output_sourced_error(ex);
        fprintf(stderr, "Run 'rethinkdb help remove-service' for help on the command\n");
    } catch (const std::exception& ex) {
        fprintf(stderr, "%s\n", ex.what());
    }
    return EXIT_FAILURE;
}

#endif /* _WIN32 */
