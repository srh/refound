#ifndef CLUSTERING_ID_TYPES_HPP_
#define CLUSTERING_ID_TYPES_HPP_

#include "containers/uuid.hpp"
#include "rpc/serialize_macros.hpp"

struct contract_id_t {
    uuid_u value;
};
// namespace_id_t and database_id_t are defined in uuid.hpp because they were typedefed
// there previously.

RDB_MAKE_SERIALIZABLE_1(contract_id_t, value);  // Could be declared/impled jsut for cluster.
RDB_MAKE_SERIALIZABLE_1(namespace_id_t, value);
RDB_MAKE_SERIALIZABLE_1(database_id_t, value);
RDB_MAKE_SERIALIZABLE_1(branch_id_t, value);

#define RDB_ID_TYPES_OP(typ, op) \
    inline bool operator op(const typ &x, const typ &y) { \
        return x.value op y.value; \
    } \
    extern int rdb_id_types_force_semicolon_declaration

RDB_ID_TYPES_OP(contract_id_t, <);
RDB_ID_TYPES_OP(namespace_id_t, <);
RDB_ID_TYPES_OP(database_id_t, <);
RDB_ID_TYPES_OP(branch_id_t, <);

RDB_ID_TYPES_OP(contract_id_t, ==);
RDB_ID_TYPES_OP(contract_id_t, !=);
RDB_ID_TYPES_OP(namespace_id_t, ==);
RDB_ID_TYPES_OP(namespace_id_t, !=);
RDB_ID_TYPES_OP(database_id_t, ==);
RDB_ID_TYPES_OP(database_id_t, !=);
RDB_ID_TYPES_OP(branch_id_t, ==);
RDB_ID_TYPES_OP(branch_id_t, !=);

#undef RDB_ID_TYPES_OP

inline std::string uuid_to_str(const namespace_id_t &x) {
    return uuid_to_str(x.value);
}
inline std::string uuid_to_str(const database_id_t &x) {
    return uuid_to_str(x.value);
}
inline std::string uuid_to_str(const branch_id_t &x) {
    return uuid_to_str(x.value);
}

inline void debug_print(printf_buffer_t *buf, const contract_id_t &x) {
    debug_print(buf, x.value);
}
inline void debug_print(printf_buffer_t *buf, const branch_id_t &x) {
    debug_print(buf, x.value);
}

#endif  // CLUSTERING_ID_TYPES_HPP_
