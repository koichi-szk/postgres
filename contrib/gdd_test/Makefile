#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for contrib/gdd_test
#
#-------------------------------------------------------------------------

# contrib/gdd_test/Makefile

MODULE_big = gdd_test

OBJS = external_lock_test.o gdd_test_external_lock.o

PGFILEDESC = "gdd_test: test global deadlock detectoin."

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gdd_test
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

