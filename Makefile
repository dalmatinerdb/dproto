REBAR = $(shell pwd)/rebar3

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

qc:
	$(REBAR) as eqc eqc

test: all
	$(REBAR) eunit

###
### Docs
###
docs:
	$(REBAR) skip_deps=true doc

##
## Developer targets
##

xref:
	$(REBAR) xref
