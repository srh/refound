// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/terms.hpp"

#include <vector>

#include "rdb_protocol/error.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/op.hpp"

namespace ql {

// AND and OR are written strangely because I originally thought that we could
// have non-boolean values that evaluate to true, but then we decided not to do
// that.

class and_term_t : public op_term_t {
public:
    and_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(0, -1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        scoped_ptr_t<val_t> v = new_val_bool(true);
        for (size_t i = 0; i < args->num_args(); ++i) {
            v = args->arg(env, i);
            if (!v->as_bool(env)) break;
        }
        return v;
    }
    virtual const char *name() const { return "and"; }
};

class or_term_t : public op_term_t {
public:
    or_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(0, -1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        scoped_ptr_t<val_t> v = new_val_bool(false);
        for (size_t i = 0; i < args->num_args(); ++i) {
            v = args->arg(env, i);
            if (v->as_bool(env)) break;
        }
        return v;
    }
    virtual const char *name() const { return "or"; }
};

class branch_term_t : public op_term_t {
public:
    branch_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(3, -1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        rcheck(args->num_args() % 2 == 1,
               base_exc_t::LOGIC,
               "Cannot call `branch` term with an even number of arguments.");

        for (size_t i = 0; i < args->num_args()-1; i += 2) {
            scoped_ptr_t<val_t> v = args->arg(env, i);
            if (v->as_bool(env)) {
                return args->arg(env, i+1);
            }
        }

        return args->arg(env, args->num_args()-1);
    }
    virtual const char *name() const { return "branch"; }
};


class funcall_term_t : public op_term_t {
public:
    funcall_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1, -1),
          optargspec_t({"_SHORTCUT_", "_EVAL_FLAGS_"})) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        function_shortcut_t shortcut = CONSTANT_SHORTCUT;
        eval_flags_t flags = eval_flags_t::NO_FLAGS;
        if (scoped_ptr_t<val_t> v = args->optarg(env, "_SHORTCUT_")) {
            shortcut = static_cast<function_shortcut_t>(v->as_int(env));
        }

        if (scoped_ptr_t<val_t> v = args->optarg(env, "_EVAL_FLAGS_")) {
            flags = static_cast<eval_flags_t>(v->as_int(env));
        }

        /* This switch exists just to make sure that we don't get a bogus value
         * for the shortcut. */
        switch (shortcut) {
            case NO_SHORTCUT: // fallthru
            case CONSTANT_SHORTCUT: // fallthru
            case GET_FIELD_SHORTCUT: // fallthru
            case PLUCK_SHORTCUT: // fallthru
            case PAGE_SHORTCUT: // fallthru
                break;
            default:
                rfail(base_exc_t::INTERNAL,
                      "Unrecognized value `%d` for _SHORTCUT_ argument.", shortcut);
        }
        counted_t<const func_t> f =
            std::move(*args->arg(env, 0, flags)).as_func(env->env, shortcut);

        // We need specialized logic for `grouped_data` here because `funcall`
        // needs to be polymorphic on its second argument rather than its first.
        // (We might have wanted to do this anyway, though, because otherwise
        // we'd be compiling shortcut functions `n` times.)
        if (args->num_args() == 1) {
            return f->call(env->env, flags);
        } else {
            scoped_ptr_t<val_t> arg1 = args->arg(env, 1, flags);
            std::vector<datum_t> arg_datums(1);
            arg_datums.reserve(args->num_args() - 1);
            for (size_t i = 2; i < args->num_args(); ++i) {
                arg_datums.push_back(args->arg(env, i, flags)->as_datum(env));
            }
            r_sanity_check(!arg_datums[0].has());
            counted_t<grouped_data_t> gd
                = arg1->maybe_as_promiscuous_grouped_data(env->env);
            if (gd.has()) {
                counted_t<grouped_data_t> out(new grouped_data_t());
                // We're processing gd into another grouped_data_t, so we are
                // assuming nothing about its order.
                for (auto kv = gd->begin(); kv != gd->end(); ++kv) {
                    arg_datums[0] = kv->second;
                    (*out)[kv->first] = f->call(env->env, arg_datums, flags)->as_datum(env);
                }
                return make_scoped<val_t>(out, backtrace());
            } else {
                arg_datums[0] = arg1->as_datum(env);
                return f->call(env->env, arg_datums, flags);
            }
        }
    }
    virtual const char *name() const { return "funcall"; }
};


counted_t<term_t> make_and_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<and_term_t>(env, term);
}
counted_t<term_t> make_or_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<or_term_t>(env, term);
}
counted_t<term_t> make_branch_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<branch_term_t>(env, term);
}
counted_t<term_t> make_funcall_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<funcall_term_t>(env, term);
}

}  // namespace ql
