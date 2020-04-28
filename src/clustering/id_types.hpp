#ifndef CLUSTERING_ID_TYPES_HPP_
#define CLUSTERING_ID_TYPES_HPP_

#include "containers/uuid.hpp"
#include "rpc/serialize_macros.hpp"

RDB_MAKE_SERIALIZABLE_1(namespace_id_t, value);
RDB_MAKE_SERIALIZABLE_1(database_id_t, value);

#define RDB_ID_TYPES_OP(typ, op) \
    inline bool operator op(const typ &x, const typ &y) { \
        return x.value op y.value; \
    } \
    extern int rdb_id_types_force_semicolon_declaration

RDB_ID_TYPES_OP(namespace_id_t, <);
RDB_ID_TYPES_OP(database_id_t, <);

RDB_ID_TYPES_OP(namespace_id_t, ==);
RDB_ID_TYPES_OP(namespace_id_t, !=);
RDB_ID_TYPES_OP(database_id_t, ==);
RDB_ID_TYPES_OP(database_id_t, !=);

#undef RDB_ID_TYPES_OP

namespace std {
template<> struct hash<database_id_t> {
    size_t operator()(const database_id_t& x) const {
        return std::hash<uuid_u>()(x.value);
    }
};
}  // namespace std

inline std::string uuid_to_str(const namespace_id_t &x) {
    return uuid_to_str(x.value);
}
inline std::string uuid_to_str(const database_id_t &x) {
    return uuid_to_str(x.value);
}

#endif  // CLUSTERING_ID_TYPES_HPP_
