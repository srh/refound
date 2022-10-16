// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef CONTAINERS_SCOPED_HPP_
#define CONTAINERS_SCOPED_HPP_

#include <string.h>

#include <utility>

#include "config/args.hpp"
#include "errors.hpp"
#include "memory_utils.hpp"

// Like boost::scoped_ptr only with release, init, no bool conversion, no boost headers!
template <class T>
class scoped_ptr_t {
public:
    template <class U>
    friend class scoped_ptr_t;

    scoped_ptr_t() : ptr_(nullptr) { }
    explicit scoped_ptr_t(T *p) : ptr_(p) { }

    // (These noexcepts don't actually do anything w.r.t. STL containers, since the
    // type's not copyable.  There is no specific reason why these are many other
    // functions need be marked noexcept with any degree of urgency.)
    scoped_ptr_t(scoped_ptr_t &&movee) noexcept : ptr_(movee.ptr_) {
        movee.ptr_ = nullptr;
    }
    template <class U>
    scoped_ptr_t(scoped_ptr_t<U> &&movee) noexcept : ptr_(movee.ptr_) {
        movee.ptr_ = nullptr;
    }

    ~scoped_ptr_t() {
        reset();
    }

    scoped_ptr_t &operator=(scoped_ptr_t &&movee) noexcept {
        scoped_ptr_t tmp(std::move(movee));
        swap(tmp);
        return *this;
    }

    template <class U>
    scoped_ptr_t &operator=(scoped_ptr_t<U> &&movee) noexcept {
        scoped_ptr_t tmp(std::move(movee));
        swap(tmp);
        return *this;
    }

    // These 'init' functions are largely obsolete, because move semantics are a
    // better thing to use.
    template <class U>
    void init(scoped_ptr_t<U> &&movee) {
        rassert(ptr_ == NULL);

        operator=(std::move(movee));
    }

    // includes a sanity-check for first-time use.
    void init(T *value) {
        rassert(ptr_ == NULL);

        // This is like reset with an assert.
        T *tmp = ptr_;
        ptr_ = value;
        delete tmp;
    }

    void reset() {
        T *tmp = ptr_;
        ptr_ = nullptr;
        delete tmp;
    }

    MUST_USE T *release() {
        T *tmp = ptr_;
        ptr_ = nullptr;
        return tmp;
    }

    void swap(scoped_ptr_t &other) noexcept {
        T *tmp = ptr_;
        ptr_ = other.ptr_;
        other.ptr_ = tmp;
    }

    T &operator*() const {
        rassert(ptr_);
        return *ptr_;
    }

    T *get() const {
        rassert(ptr_);
        return ptr_;
    }

    T *get_or_null() const {
        return ptr_;
    }

    T *operator->() const {
        rassert(ptr_);
        return ptr_;
    }

    bool has() const {
        return ptr_ != nullptr;
    }

    explicit operator bool() const {
        return ptr_ != nullptr;
    }

private:
    T *ptr_;

    DISABLE_COPYING(scoped_ptr_t);
};

template <class T> using scoped = scoped_ptr_t<T>;

template <class T, class... Args>
scoped<T> make_scoped(Args&&... args) {
    return scoped<T>(new T(std::forward<Args>(args)...));
}

// Not really like boost::scoped_array.  A fascist array.
template <class T>
class scoped_array_t {
public:
    scoped_array_t() : ptr_(nullptr), size_(0) { }
    explicit scoped_array_t(size_t n) : ptr_(nullptr), size_(0) {
        init(n);
    }

    scoped_array_t(T *ptr, size_t _size) : ptr_(nullptr), size_(0) {
        init(ptr, _size);
    }

    // (These noexcepts don't actually do anything w.r.t. STL containers, since the
    // type's not copyable.  There is no specific reason why these are many other
    // functions need be marked noexcept with any degree of urgency.)
    scoped_array_t(scoped_array_t &&movee) noexcept
        : ptr_(movee.ptr_), size_(movee.size_) {
        movee.ptr_ = nullptr;
        movee.size_ = 0;
    }

    ~scoped_array_t() {
        reset();
    }

    scoped_array_t &operator=(scoped_array_t &&movee) noexcept {
        scoped_array_t tmp(std::move(movee));
        swap(tmp);
        return *this;
    }

    void init(size_t n) {
        rassert(ptr_ == NULL);
        ptr_ = new T[n];
        size_ = n;
    }

    // The opposite of release.
    void init(T *ptr, size_t _size) {
        rassert(ptr != NULL);
        rassert(ptr_ == NULL);

        ptr_ = ptr;
        size_ = _size;
    }

    void reset() {
        T *tmp = ptr_;
        ptr_ = nullptr;
        size_ = 0;
        delete[] tmp;
    }

    MUST_USE T *release(size_t *size_out) {
        *size_out = size_;
        T *tmp = ptr_;
        ptr_ = NULL;
        size_ = 0;
        return tmp;
    }

    void swap(scoped_array_t &other) noexcept {
        T *tmp = ptr_;
        size_t tmpsize = size_;
        ptr_ = other.ptr_;
        size_ = other.size_;
        other.ptr_ = tmp;
        other.size_ = tmpsize;
    }

    T &operator[](size_t i) const {
        rassert(ptr_);
        rassert(i < size_);
        return ptr_[i];
    }

    T *data() const {
        rassert(ptr_);
        return ptr_;
    }

    size_t size() const {
        rassert(ptr_);
        return size_;
    }

    bool has() const {
        return ptr_ != NULL;
    }

private:
    T *ptr_;
    size_t size_;

    DISABLE_COPYING(scoped_array_t);
};

/*
 * For pointers with custom allocators and deallocators
 */
template <class T, void*(*alloc)(size_t), void(*dealloc)(void*)>
class scoped_alloc_t {

public:
    template <class U, void*(*alloc_)(size_t), void(*dealloc_)(void*)>
    friend class scoped_alloc_t;

    scoped_alloc_t() : ptr_(nullptr) { }
    explicit scoped_alloc_t(size_t n) : ptr_(static_cast<T *>(alloc(n))) { }
    scoped_alloc_t(const void *beg, size_t n) {
        ptr_ = static_cast<T *>(alloc(n));
        memcpy(ptr_, beg, n);
    }
    // (These noexcepts don't actually do anything w.r.t. STL containers, since the
    // type's not copyable.  There is no specific reason why these are many other
    // functions need be marked noexcept with any degree of urgency.)
    scoped_alloc_t(scoped_alloc_t &&movee) noexcept
        : ptr_(movee.ptr_) {
        movee.ptr_ = nullptr;
    }

    template <class U>
    scoped_alloc_t(scoped_alloc_t<U, alloc, dealloc> &&movee) noexcept
        : ptr_(movee.ptr_) {
        movee.ptr_ = nullptr;
    }

    void operator=(scoped_alloc_t &&movee) noexcept {
        scoped_alloc_t tmp(std::move(movee));
        swap(tmp);
    }

    T *get() const { return ptr_; }
    T *operator->() const { return ptr_; }

    bool has() const {
        return ptr_ != nullptr;
    }

    void reset() {
        scoped_alloc_t tmp;
        swap(tmp);
    }

    ~scoped_alloc_t() {
        static_assert(std::is_pod<T>::value || std::is_same<T, void>::value,
                      "refusing to malloc non-POD, non-void type");

        dealloc(ptr_);
    }

private:
    friend class released_t;

    explicit scoped_alloc_t(void*) = delete;

    void swap(scoped_alloc_t &other) noexcept {
        T *tmp = ptr_;
        ptr_ = other.ptr_;
        other.ptr_ = tmp;
    }

    T *ptr_;

    DISABLE_COPYING(scoped_alloc_t);
};

template <int alignment>
void *raw_malloc_aligned(size_t size) {
    return raw_malloc_aligned(size, alignment);
}

// A type for pointers using rmalloc/free
template <class T>
using scoped_malloc_t = scoped_alloc_t<T, rmalloc, free>;

#ifndef _WIN32
// A type for page-aligned pointers
template <class T>
using scoped_page_aligned_ptr_t = scoped_alloc_t<T, raw_malloc_page_aligned, raw_free_aligned>;
#endif

#endif  // CONTAINERS_SCOPED_HPP_
