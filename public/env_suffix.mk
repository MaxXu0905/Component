.SUFFIXES:.o .h
.SUFFIXES:.cpp .o
.SUFFIXES:.c .o
.SUFFIXES:.s .o

.cpp.o:
	$(CC) -c $(CFLAGS) -o $@ $<

.c.o:
	cc -c $(MFLAGS) -o $@ $<

.s.o:
	cc -c $(MFLAGS) -o $@ $<

clean:
ifdef SKIPS
	-for i in $(SKIPS); do mv $$i $$i.bak; done
endif
	-rm -rf src/*.o $(BINS) $(LIBS) $(TMPS)
ifdef SKIPS
	-for i in $(SKIPS); do mv $$i.bak $$i; done
endif

install: $(TARGET)
ifndef SAMPLES

ifdef BINS
	-mkdir -p $(APPROOT)/bin
	-for i in $(BINS); do rm -f $(APPROOT)/bin/$$i; done
	cp $(BINS) $(APPROOT)/bin/
ifndef DEBUG
	-for i in $(BINS); do $(STRIP) $(APPROOT)/bin/$$i; done
endif
endif

ifdef LIBS
	-mkdir -p $(APPROOT)/lib
	-for i in $(LIBS); do rm -f $(APPROOT)/lib/$$i; done
	cp $(LIBS) $(APPROOT)/lib/
ifndef DEBUG
	-for i in $(LIBS); do $(STRIP) $(APPROOT)/lib/$$i; done
endif
endif

ifdef INCLUDES
	echo "$(INCLUDES)"
	-mkdir -p $(APPROOT)/include
	-for i in $(INCLUDES); do rm -f $(APPROOT)/include/$$i; done
	cp $(INCLUDES) $(APPROOT)/include/
endif

else

	-rm -rf $(APPROOT)/samples/$(SAMPLES)
	-mkdir -p $(APPROOT)/samples/$(SAMPLES)
	-cp -R . $(APPROOT)/samples/$(SAMPLES)/
	-rm -rf $(APPROOT)/samples/$(SAMPLES)/.svn $(APPROOT)/samples/$(SAMPLES)/*/.svn

endif

remake: 
	make clean all
