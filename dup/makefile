include $(SGROOT)/etc/env_public.mk
CFLAGS = $(PCFLAGS) -I$(SQLITE_ROOT)/include -I$(APPROOT)/include
LDFLAGS = $(PLDFLAGS) -L$(SQLITE_ROOT)/lib -lsqlite3 -L$(APPROOT)/lib -lal
BINS = dup
LIBS = libdup.$(SO)
TARGET = $(LIBS) $(BINS)

OBJS = src/dup_ctx.o src/dup_base.o
SOBJS = src/dup_ctx.o src/dup_svr.o src/dup_svc.o

all:$(TARGET) install

libdup.$(SO):$(OBJS)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(OBJS) -L. -lsa $(LDFLAGS)

dup:$(SOBJS)
	sgcompile --server --output=$@ --first="$(CFLAGS) $(SOBJS)" --server-class=dup_svr --operation=.DUP:dup_svc --namespace=ai::app --last="-L. -lsvc $(LDFLAGS)" --system

include ../public/env_suffix.mk
