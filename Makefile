SHELL := /usr/bin/env bash
PACKAGES = book engine exchange intermediate

define PROTO_RESOLVE
	python3 -m grpc_tools.protoc -Ipet_exchange/$(1)/proto/ --python_out=./pet_exchange/proto --grpc_python_out=./pet_exchange/proto ./pet_exchange/$(1)/proto/$(1).proto;
endef

.PET-EXCHANGE: install
install:
	python -m pip install wheel grpcio-tools setuptools
	$(foreach PACKAGE,$(PACKAGES),$(call PROTO_RESOLVE,$(PACKAGE)))
	python setup.py install
	mkdir -p trades

.PET-EXCHANGE: develop
develop:
	python -m pip install wheel setuptools grpcio-tools
	$(foreach PACKAGE,$(PACKAGES),$(call PROTO_RESOLVE,$(PACKAGE)))
	pip install -e .[dev]
	mkdir -p trades
