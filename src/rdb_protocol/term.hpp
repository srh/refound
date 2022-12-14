// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_TERM_HPP_
#define RDB_PROTOCOL_TERM_HPP_

#include "containers/counted.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/ql2proto.hpp"
#include "rdb_protocol/term_storage.hpp"
#include "rdb_protocol/types.hpp"

/* Here is some basic info about ReQL code to be aware of:

   - Compile time happens when a term_t is constructed, and runtime
     happens when eval is called on it. So evaluating arguments in the
     constructor goes poorly.

   - If a term can push work to the shards it has to be in the first
     group of term_forbids_writes.  Related code has some basic type
     checking -- grep for all appearances of a term name so you can
     see every switch statement it's used in.

   - Calling arg(0) more than once can re-evaluate the argument.
*/

namespace ql {

class datum_stream_t;
class datum_t;
class db_t;
class env_t;
class func_t;
class scope_env_t;
class table_t;
class table_slice_t;
class var_captures_t;
class compile_env_t;
class deterministic_t;
class val_t;

class runtime_term_t : public slow_atomic_countable_t<runtime_term_t>,
                       public bt_rcheckable_t {
public:
    virtual ~runtime_term_t();
    scoped_ptr_t<val_t> eval(scope_env_t *env, eval_flags_t eval_flags = eval_flags_t::NO_FLAGS) const;
    virtual deterministic_t is_deterministic() const = 0;
    // TODO: Make this = 0.
    virtual bool is_fdb_only() const { return false; }
    virtual const char *name() const = 0;

    // Allocates a new value in the current environment.
    template<class... Args>
    scoped_ptr_t<val_t> new_val(Args... args) const {
        return make_scoped<val_t>(std::forward<Args>(args)..., backtrace());
    }
    scoped_ptr_t<val_t> new_val_bool(bool b) const;

protected:
    explicit runtime_term_t(backtrace_id_t bt);
private:
    scoped_ptr_t<val_t> eval_on_current_stack(
            scope_env_t *env,
            eval_flags_t eval_flags) const;

    virtual scoped_ptr_t<val_t> term_eval(scope_env_t *env, eval_flags_t) const = 0;
};

class term_t : public runtime_term_t {
public:
    explicit term_t(const raw_term_t &_src);
    virtual ~term_t();
    const raw_term_t &get_src() const;
    virtual void accumulate_captures(var_captures_t *captures) const = 0;

    // Only terms that do a simple selection of a field, or an array of fields,
    // are allowed to be not updated with reql version. This is checked in is_acceptable_outdated
    // in sindex_manager.
    virtual bool is_simple_selector() const { return false; }

protected:
    // Union term is a friend so we can steal arguments from an array in an optarg.
    friend class union_term_t;
    virtual const std::vector<counted_t<const term_t> > &get_original_args() const {
        rfail(base_exc_t::INTERNAL,
               "This is in term_t to allow stealing args from an"
               "optarg in union_term_t. Only call this on an op_term_t.");
    }

private:
    // The `src` member contains pointers to the original query and must not exceed
    // the lifetime of that query object.
    const raw_term_t src;
    DISABLE_COPYING(term_t);
};


counted_t<const term_t> compile_term(compile_env_t *env, const raw_term_t &t);

} // namespace ql

#endif // RDB_PROTOCOL_TERM_HPP_
