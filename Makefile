# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# 	http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

ROOT := $(shell pwd)

all: build

SCRIPT_PATH := $(ROOT)/scripts/:${PATH}
SOURCES := $(shell find . -name '*.go')
PLUGIN_BINARY := ./bin/cloudwatch.so

.PHONY: build
build: $(PLUGIN_BINARY)

$(PLUGIN_BINARY): $(SOURCES)
	PATH=${PATH} golint ./cloudwatch
	mkdir -p ./bin
	go build -buildmode c-shared -o ./bin/cloudwatch.so ./
	@echo "Built Amazon CloudWatch Logs Fluent Bit Plugin"

.PHONY: generate
generate: $(SOURCES)
	PATH=$(SCRIPT_PATH) go generate ./...


.PHONY: test
test:
	go test -timeout=120s -v -cover ./...

.PHONY: clean
clean:
	rm -rf ./bin/*
