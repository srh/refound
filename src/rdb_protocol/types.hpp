#ifndef RETHINKDB_RDB_PROTOCOL_TYPES_HPP_
#define RETHINKDB_RDB_PROTOCOL_TYPES_HPP_

namespace ql {
// TODO: Make this an enum class.
enum eval_flags_t {
    NO_FLAGS = 0,
    LITERAL_OK = 1,
};
enum class constant_now_t { no, yes };

enum class single_server_t { no, yes };


class deterministic_t {
public:
    // Is non-deterministic.
    static deterministic_t no() { return deterministic_t(1); }

    // Is non-deterministic if run across the cluster (different cpus, compilers,
    // libc's), but is deterministic on a single server.  Example: geo operations.
    static deterministic_t single_server() { return deterministic_t(2); }

    // Is non-deterministic if r.now is non-constant.
    static deterministic_t constant_now() { return deterministic_t(4); }

    // Is always deterministic.
    static deterministic_t always() { return deterministic_t(0); }

    // Computes the combined deterministic-ness of two expressions.
    deterministic_t join(deterministic_t other) const {
        return deterministic_t(bitset | other.bitset);
    }

    // Params tell the situation:
    //  - ss: are we running the term on a single server?
    //  - cn: is r.now() constant?
    // Returns true if the expression is deterministic (under the given conditions).
    // ("The expression" is whatever expression this deterministic_t value was
    // computed from.)
    bool test(single_server_t ss, constant_now_t cn) const {
        // Turn off the bits that don't apply.
        int mask = (ss == single_server_t::yes ? single_server().bitset : 0)
            | (cn == constant_now_t::yes ? constant_now().bitset : 0);
        int remaining_bits = bitset & ~mask;
        return remaining_bits == 0;
    }

private:
    explicit deterministic_t(int bits) : bitset(bits) { }

    int bitset;
};

}  // namespace ql

#endif  // RETHINKDB_RDB_PROTOCOL_TYPES_HPP_
