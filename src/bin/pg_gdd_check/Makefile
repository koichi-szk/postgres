#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_gdd_check
#
# Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_gdd_check/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_gdd_check - the PostgreSQL interactive terminal"
PGAPPICON=win32

subdir = src/bin/pg_gdd_check
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

REFDOCDIR= $(top_srcdir)/doc/src/sgml/ref

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

OBJS=	pg_gdd_check.o \
	$(WIN32RES)


all: pg_gdd_check

pg_gdd_check: $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

# help.o: sql_help.h

# See notes in src/backend/parser/Makefile about the following two rules
# sql_help.c: sql_help.h
# touch $@

# sql_help.h: create_help.pl $(wildcard $(REFDOCDIR)/*.sgml)
# $(PERL) $< $(REFDOCDIR) $*

# psqlscanslash.c: FLEXFLAGS = -Cfe -p -p
# psqlscanslash.c: FLEX_NO_BACKUP=yes
# psqlscanslash.c: FLEX_FIX_WARNING=yes

# distprep: sql_help.h sql_help.c psqlscanslash.c

install: all installdirs
	$(INSTALL_PROGRAM) pg_gdd_check$(X) '$(DESTDIR)$(bindir)/pg_gdd_check$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_gdd_check$(X)'

clean distclean:
	rm -f pg_gdd_check$(X) $(OBJS)

# files removed here are supposed to be in the distribution tarball,
# so do not clean them in the clean/distclean rules
maintainer-clean: distclean
	rm -f sql_help.h sql_help.c psqlscanslash.c
