BASEDIR = $(shell pwd)
REBAR = rebar3

APPNAME = $(shell basename $(BASEDIR))
ifneq ($(APPNAME), edbfly)
  APPNAME = edbfly
endif

RELPATH = _build/default/bin

.DEFAULT_GOAL := help

.PHONY: run

compile: ## compile
	$(REBAR) compile

eunit: ## eunit
	$(REBAR) eunit

xref: ## xref
	$(REBAR) xref

dialyzer: ## dialyzer
	$(REBAR) dialyzer

release: xref ## create escript
	$(REBAR) escriptize

run: release ## run escript
	$(RELPATH)/$(APPNAME) $(filter-out $@,$(MAKECMDGOALS))
%:
	@:

clean: ## clean
	$(REBAR) clean
	rm -rf _build

help: ## Display help information
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
