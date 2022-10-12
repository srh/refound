#ifndef PATHS_HPP_
#define PATHS_HPP_

#include <string>

#include "errors.hpp"

#ifdef _WIN32
#define PATH_SEPARATOR "\\"
#else
#define PATH_SEPARATOR "/"
#endif

// Contains the name of the directory in which all data is stored.
class base_path_t {
public:
    // Constructs an empty path.
    base_path_t() { }
    explicit base_path_t(const std::string& path);
    const std::string& path() const;

    // Make this base_path_t into an absolute path (useful for daemonizing)
    // This can only be done if the path already exists, which is why we don't do it at construction
    MUST_USE base_path_t make_absolute() const;
private:
    std::string path_;
};

// This can only be done if the path already exists.
std::string make_absolute(const std::string &path);

void remove_directory_recursive(const char *path);

std::string blocking_read_file(const char *path);
bool blocking_read_file(const char *path, std::string *contents_out);

#endif  // PATHS_HPP_
