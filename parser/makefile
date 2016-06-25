include $(SGROOT)/etc/env_public.mk
CFLAGS = $(PCFLAGS) -I$(BOOST_ROOT)  -I$(APPROOT)/include
LDFLAGS = $(PLDFLAGS) -lgp -lsg -L$(BOOST_ROOT)/stage/lib -lboost_system -lboost_thread \
	-lboost_program_options -lboost_filesystem -lboost_chrono -L$(APPROOT)/lib -lal
OBJS = src/parser.o src/parserlex.o src/parser_code.o src/parser_config.o src/prsp_ctx.o
POBJS = src/parser_main.o

INCLUDES =
ifeq "$(HAS_YACC)" ""
SKIPS = src/parser.cpp src/parser.cpp.h src/parserlex.cpp
else
TMPS = src/parser.cpp src/parser.cpp.h src/parserlex.cpp
endif

BINS = run_parser
LIBS = libparser.$(SO)
TARGET = $(LIBS) $(BINS)

all:$(TARGET) install

libparser.$(SO):$(OBJS)
	$(CC) $(CFLAGS) $(SOFLAGS) -o $@ $(OBJS) $(LDFLAGS)

run_parser:$(POBJS) libparser.$(SO)
	$(CC) $(CFLAGS) -o $@ $(POBJS) $(LDFLAGS) -L. -lparser

ifneq "$(HAS_YACC)" ""

src/parser.cpp: src/parser.y
	yacc -d -o $@ $<

src/parserlex.cpp: src/parser.l
	lex -o$@ $<

endif

include ../public/env_suffix.mk
