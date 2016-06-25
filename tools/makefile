include $(SGROOT)/etc/env_public.mk
CFLAGS = $(PCFLAGS) -I$(SSH2_ROOT)/include -I$(SQLITE_ROOT)/include
LDFLAGS = $(PLDFLAGS) -L$(BOOST_ROOT)/stage/lib -L$(SSH2_ROOT)/lib -lssh2 -lboost_thread -lboost_system -lsg
#BINS = sgssh test_ssh
BINS = sgssh
LIBS = libsshp.$(SO)
TARGET = $(LIBS) $(BINS) 

OBJS = src/app_main.o
TEST_OBJS = src/main_test.o
SOBJS = src/sgssh_ssh.o src/sgssh_file.o src/sgssh_sgip.o

all:$(TARGET) install

libsshp.$(SO):$(SOBJS)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(SOBJS) -L. $(LDFLAGS)

sgssh:$(OBJS) libsshp.$(SO)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS) -L. -lsshp

test_ssh:$(TEST_OBJS) libsshp.$(SO)
	$(CC) $(CFLAGS) -o $@ $(TEST_OBJS) $(LDFLAGS) -L. -lsshp

include ../public/env_suffix.mk
