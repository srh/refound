// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_MAIN_COMMAND_LINE_HPP_
#define CLUSTERING_ADMINISTRATION_MAIN_COMMAND_LINE_HPP_

#include "fdb/fdb.hpp"

void print_version_message();

// TODO: For consistency, make all these functions take argc/argv without command prefix.

int main_rethinkdb_create(int argc, char *argv[]);  // argc/argv passed without command prefix.
int main_rethinkdb_serve(int argc, char *argv[]);  // argc/argv passed without command prefix.
int main_rethinkdb_export(int argc, char *argv[]);
int main_rethinkdb_import(int argc, char *argv[]);
int main_rethinkdb_dump(int argc, char *argv[]);
int main_rethinkdb_restore(int argc, char *argv[]);
int main_rethinkdb_index_rebuild(int argc, char *argv[]);
int main_rethinkdb_repl(int argc, char *argv[]);
#ifdef _WIN32
int main_rethinkdb_run_service(int argc, char *argv[]);
int main_rethinkdb_install_service(int argc, char *argv[]);
int main_rethinkdb_remove_service(int argc, char *argv[]);
#endif /* _WIN32 */

void help_rethinkdb_create();
void help_rethinkdb_serve();
void help_rethinkdb_proxy();
void help_rethinkdb_porcelain();
void help_rethinkdb_export();
void help_rethinkdb_import();
void help_rethinkdb_dump();
void help_rethinkdb_restore();
void help_rethinkdb_index_rebuild();
void help_rethinkdb_repl();
#ifdef _WIN32
void help_rethinkdb_install_service();
void help_rethinkdb_remove_service();
#endif /* _WIN32 */

#endif /* CLUSTERING_ADMINISTRATION_MAIN_COMMAND_LINE_HPP_ */
