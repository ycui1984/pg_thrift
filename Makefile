EXTENSION = pg_thrift
MODULE_big = pg_thrift
OBJS = pg_thrift.o
DATA = pg_thrift--1.0.sql
REGRESS = pg_thrift

PG_CPPFLAGS = -g -O2 -Wall
SHLIB_LINK =

ifndef PG_CONFIG
	PG_CONFIG := pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
