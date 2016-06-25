include $(SGROOT)/etc/env_public.mk
CFLAGS = $(PCFLAGS) -I$(SQLITE_ROOT)/include -I$(APPROOT)/include
LDFLAGS = $(PLDFLAGS) -L$(APPROOT)/lib -ldbcclient -lal
ifeq "$(OS)" "AIX"
SOFLAGS := $(SOFLAGS) -bE:$(SGROOT)/etc/so.exp
endif
BINS = bps
LIBS = libbps.$(SO) libsearch.$(SO)
TARGET = $(LIBS) $(BINS)

OBJS = src/bps_base.o
SOBJS = src/bps_ctx.o src/bps_svr.o src/bps_svc.o
LOBJS = src/search.o

all:$(TARGET) install

libbps.$(SO):$(OBJS)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(OBJS) -L. -lsa $(LDFLAGS)

libsearch.$(SO):$(LOBJS)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(LOBJS) $(LDFLAGS)

bps:$(SOBJS)
	sgcompile --server --output=$@ --first="$(CFLAGS) $(SOBJS)" --server-class=bps_svr --operation=.BPS:bps_svc --namespace=ai::app --last="-L. -lsvc $(LDFLAGS)" --system

include ../public/env_suffix.mk
