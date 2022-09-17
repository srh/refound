#ifndef MATH_HPP_
#define MATH_HPP_

#include <math.h>
#include <stdint.h>

#include <limits>
#include <type_traits>

template <class T1, class T2>
T1 ceil_aligned(T1 value, T2 alignment) {
    static_assert(std::is_unsigned<T1>::value, "ceil_aligned expects unsigned type in first position");
    return value + alignment - (((value + alignment - 1) % alignment) + 1);
}

template <class T1, class T2>
T1 ceil_divide(T1 dividend, T2 alignment) {
    static_assert(std::is_unsigned<T1>::value, "ceil_divide expects unsigned type in first position");
    return (dividend + alignment - 1) / alignment;
}

template <class T1, class T2>
T1 floor_aligned(T1 value, T2 alignment) {
    static_assert(std::is_unsigned<T1>::value, "floor_aligned expects unsigned type in first position");
    return value - (value % alignment);
}

template <class T1, class T2>
T1 ceil_modulo(T1 value, T2 alignment) {
    // We have "x < 0" logic where which gets unused.
    static_assert(std::is_unsigned<T1>::value, "asserting ceil_modulo is currently only called with unsigned types");
    T1 x = (value + alignment - 1) % alignment;
    return value + alignment - ((x < 0 ? x + alignment : x) + 1);
}

template <class T>
T add_rangeclamped(T x, T y) {
    static_assert(std::is_unsigned<T>::value, "add_rangeclamped expects unsigned type");
    return x <= std::numeric_limits<T>::max() - y ? x + y : std::numeric_limits<T>::max();
}

template <class T>
T clamp(T x, T lo, T hi) {
    return x < lo ? lo : x > hi ? hi : x;
}

constexpr inline bool divides(int64_t x, int64_t y) {
    return y % x == 0;
}

int64_t int64_round_up_to_power_of_two(int64_t x);
uint64_t uint64_round_up_to_power_of_two(uint64_t x);

/* Forwards to the isfinite macro, or std::isfinite. */
bool risfinite(double);

/* Translates to and from `0123456789ABCDEF`. */
bool hex_to_int(char c, int *out);
char int_to_hex(int i);

#endif  // MATH_HPP_
