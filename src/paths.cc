#include "paths.hpp"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef _WIN32
#include "windows.hpp"
#include <io.h>     // NOLINT
#include <direct.h> // NOLINT
#ifndef __MINGW32__
#include <filesystem>
#endif
#else  // _WIN32
#include <ftw.h>
#endif  // _WIN32

#include "arch/io/disk.hpp"
#include "clustering/main/directory_lock.hpp"
#include "errors.hpp"
#include "logger.hpp"

#ifdef _MSC_VER

int remove_directory_helper(const char *path) {
    logNTC("In recursion: removing file '%s'\n", path);
    DWORD attrs = GetFileAttributes(path);
    if (GetLastError() == ERROR_FILE_NOT_FOUND) {
        logWRN("Trying to delete non-existent file '%s'", path);
        return 0;
    } else {
        guarantee_winerr(attrs != INVALID_FILE_ATTRIBUTES, "GetFileAttributes failed");
    }
    BOOL res;
    if (attrs & FILE_ATTRIBUTE_DIRECTORY) {
        res = RemoveDirectory(path);
    } else {
        res = DeleteFile(path);
    }
    if (res == 0) {
        fail_due_to_user_error("failed to remove: '%s': %s", path, winerr_string(GetLastError()).c_str());
    }
    return 0;
}

#else

int remove_directory_helper(const char *path, UNUSED const struct stat *, UNUSED int, UNUSED struct FTW *) {
    logNTC("In recursion: removing file '%s'\n", path);
    int res = ::remove(path);
    if (res != 0) {
        fail_due_to_user_error("Fatal error: failed to delete '%s'.", path);
    }
    return 0;
}

#endif

void remove_directory_recursive(const char *dirpath) {
#ifdef _MSC_VER
    using namespace std::tr2; // NOLINT
    std::function<void(sys::path)> go = [&go](sys::path dir){
        for (auto it : sys::directory_iterator(dir)) {
            if (sys::is_directory(it.status())) {
                go(it.path());
            } else {
                remove_directory_helper(it.path().string().c_str());
            }
        }
        remove_directory_helper(dir.string().c_str());
    };
    go(dirpath);
#else
    // max_openfd is ignored on OS X (which claims the parameter
    // specifies the maximum traversal depth) and used by Linux to
    // limit the number of file descriptors that are open (by opening
    // and closing directories extra times if it needs to go deeper
    // than that).
    // For FreeBSD, max_openfd must be >= 1 and <= OPEN_MAX. Also, even if
    // given path does not exist, delete_all_helper will still be called; so we
    // must check path existence before using nftw.
#ifdef __FreeBSD__
    const int max_openfd = OPEN_MAX;
    if (::access(dirpath, 0) != 0) return;
#else
    const int max_openfd = 128;
#endif
    logNTC("Recursively removing directory %s\n", dirpath);
    int res = nftw(dirpath, remove_directory_helper, max_openfd, FTW_PHYS | FTW_MOUNT | FTW_DEPTH);
    guarantee_err(res == 0 || get_errno() == ENOENT, "Trouble while traversing and destroying temporary directory %s.", dirpath);
#endif
}

base_path_t::base_path_t(const std::string &_path) : path_(_path) { }

std::string make_absolute(const std::string &path) {
#ifdef _WIN32
    char absolute_path[MAX_PATH];
    DWORD size = GetFullPathName(path.c_str(), sizeof(absolute_path), absolute_path, nullptr);
    guarantee_winerr(size != 0, "GetFullPathName failed");
    if (size < sizeof(absolute_path)) {
        return absolute_path;
    }
    std::string long_absolute_path;
    long_absolute_path.resize(size);
    DWORD new_size = GetFullPathName(path_.c_str(), size, &long_absolute_path[0], nullptr);
    guarantee_winerr(size != 0, "GetFullPathName failed");
    guarantee(new_size < size, "GetFullPathName: name too long");
    return long_absolute_path;
#else
    char absolute_path[PATH_MAX];
    char *res = realpath(path.c_str(), absolute_path);
    guarantee_err(res != nullptr, "Failed to determine absolute path for '%s'", path.c_str());
    return absolute_path;
#endif
}

base_path_t base_path_t::make_absolute() const {
    return base_path_t(::make_absolute(path_));
}

const std::string& base_path_t::path() const {
    guarantee(!path_.empty());
    return path_;
}

bool is_rw_with_mode(const std::string &path, mode_t st_mode_mask) {
#ifdef _WIN32
    if (_access(path.c_str(), 06 /* read and write */) != 0)
        return false;
#else
    if (access(path.c_str(), R_OK | W_OK) != 0)
        return false;
#endif
    struct stat details;
    if (stat(path.c_str(), &details) != 0)
        return false;
    return (details.st_mode & st_mode_mask) != 0;
}

bool is_rw_file(const std::string &path) {
    return is_rw_with_mode(path, S_IFREG);
}

bool is_rw_directory(const base_path_t& path) {
    return is_rw_with_mode(path.path(), S_IFDIR);
}

bool blocking_read_file(const char *path, std::string *contents_out) {
#ifdef _WIN32
    HANDLE hFile = CreateFile(path, GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, 0, nullptr);
    if (hFile == INVALID_HANDLE_VALUE) return false;
    LARGE_INTEGER fileSize;
    BOOL res = GetFileSizeEx(hFile, &fileSize);
    if (!res) {
        CloseHandle(hFile);
        return false;
    }
    DWORD remaining = fileSize.QuadPart;
    std::string ret;
    ret.resize(remaining);
    size_t index = 0;
    while (remaining > 0) {
        DWORD consumed;
        res = ReadFile(hFile, &ret[index], remaining, &consumed, nullptr);
        if (!res) {
            CloseHandle(hFile);
            return false;
        }
        remaining -= consumed;
        index += consumed;
    }
    CloseHandle(hFile);
    *contents_out = std::move(ret);
    return true;
#else
    scoped_fd_t fd;

    {
        int res;
        do {
            res = open(path, O_RDONLY);
        } while (res == -1 && get_errno() == EINTR);

        if (res == -1) {
            return false;
        }
        fd.reset(res);
    }

    std::string ret;

    char buf[4096];
    for (;;) {
        ssize_t res;
        do {
            res = read(fd.get(), buf, sizeof(buf));
        } while (res == -1 && get_errno() == EINTR);

        if (res == -1) {
            return false;
        }

        if (res == 0) {
            *contents_out = std::move(ret);
            return true;
        }

        ret.append(buf, buf + res);
    }
#endif
}

std::string blocking_read_file(const char *path) {
    std::string ret;
    bool success = blocking_read_file(path, &ret);
    guarantee(success);
    return ret;
}
