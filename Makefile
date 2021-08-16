EXTENSION  = pg_dbms_job
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
		sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PGFILEDESC = "pg_dbms_job - Propose Oracle DBMS_JOB compatibility for PostgreSQL"

PG_CONFIG = pg_config
PG91 = $(shell $(PG_CONFIG) --version | egrep " 8\.| 9\.0" > /dev/null && echo no || echo yes)

ifeq ($(PG91),yes)
DOCS = $(wildcard README*)
SCRIPTS = bin/*
MODULES =

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
else
$(error Minimum version of PostgreSQL required is 9.1.0)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

install: distconf

distconf:
	install -D --mode=600 --owner=postgres etc/$(EXTENSION).conf /etc/$(EXTENSION)/$(EXTENSION).conf.dist

installcheck:
	$(PROVE)
