include $(SGROOT)/etc/env_public.mk
CFLAGS = $(PCFLAGS) -I$(SQLITE_ROOT)/include -I$(ACTIVEMQ_CPP_ROOT)/include
ifdef SCHD_DEBUG
CFLAGS := $(CFLAGS) -DSCHD_DEBUG
endif
LDFLAGS = $(PLDFLAGS) -lgp -lsg -L$(BOOST_ROOT)/stage/lib -lboost_system -lboost_thread \
	-lboost_program_options -lboost_filesystem -lboost_chrono -lsqlcommon \
	-L$(SQLITE_ROOT)/lib -lsqlite3 -L$(ACTIVEMQ_CPP_ROOT)/lib -lactivemq-cpp
LIBS = libal.$(SO) libsa.$(SO) libsvc.$(SO) libschd.$(SO)
BINS = run_integrator schd run_schd run_log2jms schd_agent
ifdef ORACLE_HOME
BINS := $(BINS) db2int
endif
TMPS = app.mo
ETCS = app.po
TARGET = $(LIBS) $(BINS) app.mo

LOCALE_OBJS = src/app_locale.o
OBJS = src/sap_ctx.o src/sat_ctx.o src/sa_base.o src/sa_input.o src/sa_output.o \
	src/sa_config.o src/sa_manager.o src/dblog.o src/sa_istream.o src/sa_itext.o \
	src/sa_ifixed.o src/sa_ibinary.o src/sa_ivariable.o src/sa_iasn1.o src/sa_idb.o \
	src/sa_isocket.o src/sa_iconnection.o src/sa_isfixed.o src/sa_isvariable.o \
	src/sa_ostream.o src/sa_osource.o src/sa_odbsource.o src/sa_otarget.o \
	src/sa_odbtarget.o src/sa_ostarget.o src/sa_osummary.o src/sa_ostat.o \
	src/sa_global.o
SOBJS = src/svcp_ctx.o src/svc_base.o
IOBJS= src/sa_integrator.o
DB2INT_OBJS = src/db2int.o
SCHD_LOBJS = src/schd_ctx.o src/schd_rpc.o src/schd_policy.o
SCHD_SOBJS = src/schd_svr.o src/schd_svc.o
SCHD_COBJS = src/schd_config.o src/schd_clt.o
SCHD_AGENT = src/schd_agent.o
LOG2JMS_OBJS = src/schd_log2jms.o

all:$(TARGET) install

libal.$(SO):$(LOCALE_OBJS)
	$(CC) $(PCFLAGS) $(SOFLAGS) -o $@ $(LOCALE_OBJS) $(PLDFLAGS)

libsa.$(SO):$(OBJS) libal.$(SO)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(OBJS) $(LDFLAGS) -L. -lal

libsvc.$(SO):$(SOBJS) libal.$(SO)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(SOBJS) $(LDFLAGS) -L. -lal

libschd.$(SO):$(SCHD_LOBJS) libsa.$(SO)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(SCHD_LOBJS) $(LDFLAGS) -L. -lsa

run_integrator:$(IOBJS) libal.$(SO) libsa.$(SO) libschd.$(SO)
	sgcompile --client --output=$@ --first="$(CFLAGS) $(IOBJS) -L. -lal -lsa -lschd -ldbc" --last="$(LDFLAGS)"

db2int:$(DB2INT_OBJS) libal.$(SO)
	$(CC) $(CFLAGS) -o $@ $(DB2INT_OBJS) -L. -ldbc -lsqloracle -lpubapp -lal $(LDFLAGS)

schd:$(SCHD_SOBJS) libal.$(SO) libschd.$(SO)
	sgcompile --server --output=$@ --first="$(CFLAGS) $(SCHD_SOBJS) -L. -lal -lschd" --server-class="schd_svr" --operation="SCHD_SVC:schd_svc" --namespace=ai::app --last="$(LDFLAGS)"

run_schd:$(SCHD_COBJS) libal.$(SO) libschd.$(SO)
	sgcompile --client --output=$@ --first="$(CFLAGS) $(SCHD_COBJS) -L. -lal -lschd" --last="$(LDFLAGS)"

schd_agent:$(SCHD_AGENT) libal.$(SO) libschd.$(SO)
	sgcompile --client --output=$@ --first="$(CFLAGS) $(SCHD_AGENT) -L. -lal -lschd" --last="$(LDFLAGS)"

run_log2jms:$(LOG2JMS_OBJS) libal.$(SO) libschd.$(SO)
	$(CC) $(CFLAGS) -o $@ $(LOG2JMS_OBJS) $(LDFLAGS) -L. -lal -lschd

app.mo: app.po
	msgfmt -o $@ $<
	mkdir -p $(APPROOT)/etc/zh_CN/LC_MESSAGES
	cp $@ $(APPROOT)/etc/zh_CN/LC_MESSAGES/

text:
	find .. -name *.cpp -o -name *.h > all_files.txt
	xgettext --keyword=_ --from-code=GBK --no-location -f all_files.txt -x app.po -o app_new.po
	rm -f all_files.txt

include env_suffix.mk
