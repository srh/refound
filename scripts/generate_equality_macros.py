#!/usr/bin/env python
# Copyright 2010-2013 RethinkDB, all rights reserved.

"""This script is used to generate the
RDB_MAKE_EQUALITY_COMPARABLE_*() macro definitions.

This script is meant to be run as follows (assuming you are in the
"rethinkdb/src" directory):

$ ../scripts/generate_equality_macros.py > rpc/equality_macros.hpp

"""

from __future__ import print_function

import sys

try:
    xrange
except NameError:
    xrange = range

def help_generate_equality_comparable_macro(nfields, impl):
    print("#define RDB_%s_EQUALITY_COMPARABLE_%d(type_t%s) \\" % \
        (("IMPL" if impl else "MAKE"), nfields, "".join(", field%d" % (i + 1) for i in xrange(nfields))))
    unused = "UNUSED " if nfields == 0 else ""
    print("    %sbool operator==(%sconst type_t &_a_, %sconst type_t &_b_) { \\" % ("" if impl else "inline ", unused, unused))
    if nfields == 0:
        print("        return true; \\")
    else:
        print("        return " + " && ".join("_a_.field%d == _b_.field%d" % (i + 1, i + 1) for i in xrange(nfields)) + "; \\")
    print("    } \\")
    # Putting this here makes us require a semicolon after macro invocation.
    print("    extern int equality_force_semicolon_declaration")

def generate_make_equality_comparable_macro(nfields):
    help_generate_equality_comparable_macro(nfields, False)

def generate_impl_equality_comparable_macro(nfields):
    help_generate_equality_comparable_macro(nfields, True)

def generate_make_me_equality_comparable_macro(nfields):
    print("#define RDB_MAKE_ME_EQUALITY_COMPARABLE_%d(type_t%s) \\" % \
        (nfields, "".join(", field%d" % (i + 1) for i in xrange(nfields))))
    unused = "UNUSED " if nfields == 0 else ""
    print("    bool operator==(%sconst type_t &_a_) const { \\" % unused)
    if nfields == 0:
        print("        return true; \\")
    else:
        print("        return " + " && ".join("field%d == _a_.field%d" % (i + 1, i + 1) for i in xrange(nfields)) + "; \\")
    print("    } \\")
    print("    friend class equality_force_semicolon_declaration_t")

if __name__ == "__main__":

    print("// Copyright 2010-2013 RethinkDB, all rights reserved.")
    print("#ifndef RETHINKDB_RPC_EQUALITY_MACROS_HPP_")
    print("#define RETHINKDB_RPC_EQUALITY_MACROS_HPP_")
    print()

    print("/* This file is automatically generated by '%s'." % " ".join(sys.argv))
    print("Please modify '%s' instead of modifying this file.*/" % sys.argv[0])
    print()

    print("""
#define RDB_DECLARE_EQUALITY_COMPARABLE(type_t) \\
  bool operator==(const type_t &, const type_t &)
    """.strip())

    for nfields in xrange(0, 20):
        generate_make_equality_comparable_macro(nfields)
        generate_impl_equality_comparable_macro(nfields)
        generate_make_me_equality_comparable_macro(nfields)
        print()

    print("#endif  // RETHINKDB_RPC_EQUALITY_MACROS_HPP_")