#ifndef LIBPQGDDCHECKREMOTE_H
#define LIBPQGDDCHECKREMOTE_H

typedef char *(*pg_gdd_check_f) (char *connstr, char *wfg);

extern PGDLLIMPORT pg_gdd_check_f	pg_gdd_check_function;

#define pg_gdd_check(c)	pg_gdd_check_function(c);

#endif /* LIBPQGDDCHECKREMOTE_H */
