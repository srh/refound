// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_AUTHENTICATION_ERROR_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_AUTHENTICATION_ERROR_HPP

#include <stdexcept>
#include <string>

namespace auth {
    // 10: "invalid-encoding"
    // 11: "extensions-not-supported" ; unrecognized 'm' value
    // 12: "invalid-proof"
    // 13: "channel-bindings-dont-match"
    // 14: "server-does-support-channel-binding" ; server does not support channel binding
    // 15: "channel-binding-not-supported"
    // 16: "unsupported-channel-binding-type"
    // 17: "unknown-user"
    // 18: "invalid-username-encoding" ; invalid username encoding (invalid UTF-8 or SASLprep failed)
    // 19: "no-resources"
    // 20: "other-error"

class authentication_error_t : public std::runtime_error {
public:
    authentication_error_t(uint32_t error_code, std::string const &_what)
        : std::runtime_error(_what),
          m_error_code(error_code) {
    }

    uint32_t get_error_code() const {
        return m_error_code;
    }

    static constexpr uint32_t invalid_encoding = 10;
    static constexpr uint32_t extensions_not_supported = 11;
    static constexpr uint32_t invalid_proof = 12;
    static constexpr uint32_t channel_binding_not_supported = 15;
    static constexpr uint32_t unknown_user = 17;
    static constexpr uint32_t invalid_username_encoding = 18;
    static constexpr uint32_t no_resources = 19;
    static constexpr uint32_t other_error = 20;

private:
    uint32_t m_error_code;
};

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_AUTHENTICATION_ERROR_HPP
