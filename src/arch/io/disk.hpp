// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef ARCH_IO_DISK_HPP_
#define ARCH_IO_DISK_HPP_

#include "arch/io/io_utils.hpp"
#include "arch/types.hpp"
#include "concurrency/auto_drainer.hpp"
#include "containers/scoped.hpp"
#include "utils.hpp"

#include "perfmon/core.hpp"

// TODO: Remove these unused(?) options.
// The maximum concurrent IO requests per event queue.. the default value.
const int DEFAULT_MAX_CONCURRENT_IO_REQUESTS = 64;

// The maximum user-specifiable value how many concurrent I/O requests may be done per event
// queue.  (A million is a ridiculously high value, but also safely nowhere near INT_MAX.)
const int MAXIMUM_MAX_CONCURRENT_IO_REQUESTS = MILLION;

// Makes blocking syscalls.  Upon error, returns the errno value.
int perform_datasync(fd_t fd);

// Calls fsync() on the parent directory of the given path.
// Returns the errno value in case of an error and 0 otherwise.
MUST_USE int fsync_parent_directory(const char *path);
// Like fsync_parent_directory but warns the user on an error
void warn_fsync_parent_directory(const char *path);

#endif // ARCH_IO_DISK_HPP_

