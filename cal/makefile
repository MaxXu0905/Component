include $(SGROOT)/etc/env_public.mk
CFLAGS = $(PCFLAGS) -I$(SQLITE_ROOT)/include -I$(APPROOT)/include
ifdef INV_DEBUG
CFLAGS := $(CFLAGS) -DINV_DEBUG
endif
LDFLAGS = $(PLDFLAGS) -L$(APPROOT)/lib -ldbcclient -lal
BINS = 
ifdef ORACLE_HOME
BINS := $(BINS) db2cal db2stat
endif
LIBS = libcal.$(SO) libcalfunc.$(SO)
TARGET = $(LIBS) $(BINS)

OBJS = src/cal_base.o src/cal_manager.o src/cal_struct.o src/calp_ctx.o \
	src/calt_ctx.o src/cal_policy.o src/cal_rule.o src/cal_tree.o
DB2CAL_OBJS = src/db2cal.o
DB2STAT_OBJS = src/db2stat.o
LOBJS = src/cal_func.o

all:$(TARGET) install

libcal.$(SO):$(OBJS)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(OBJS) -L. -lsa $(LDFLAGS)

db2cal:$(DB2CAL_OBJS)
	$(CC) $(CFLAGS) -o $@ $(DB2CAL_OBJS) -L. -ldbc -lsqloracle -lpubapp $(LDFLAGS)

db2stat:$(DB2STAT_OBJS)
	$(CC) $(CFLAGS) -o $@ $(DB2STAT_OBJS) -L. -ldbc -lsqloracle -lpubapp $(LDFLAGS)

libcalfunc.$(SO):$(LOBJS)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(LOBJS) $(LDFLAGS)
	
include ../public/env_suffix.mk
