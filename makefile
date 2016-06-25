ARGS=all

ALL=public dup bps cal parser tools

.NOTPARALLEL: $(ALL)

all: $(ALL)
	@:

$(ALL)::
	cd $@; $(MAKE) $(ARGS)

install clean:
	@$(MAKE) ARGS=$@

remake:
	@$(MAKE) ARGS=clean
	@$(MAKE) ARGS=all
