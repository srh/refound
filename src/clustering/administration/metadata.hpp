// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_METADATA_HPP_
#define CLUSTERING_ADMINISTRATION_METADATA_HPP_

struct admin_err_t;
class name_string_t;

admin_err_t db_not_found_error(const name_string_t &name);

#endif  // CLUSTERING_ADMINISTRATION_METADATA_HPP_
