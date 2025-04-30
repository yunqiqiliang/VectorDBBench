
SHELL=/bin/bash

.DEFAULT_GOAL := default

.PHONY: clean build

VERSION = "0.1.0"

default: all ## Default target is all.

help: ## display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)



build-docker: ## Build multi-architecture Docker images
	docker buildx create --name multiarch-builder --use --driver docker-container || true
	docker buildx inspect --bootstrap
	docker buildx build --platform linux/amd64,linux/arm64 \
        --build-arg BASE_IMAGE=python:3.11-slim \
        -t czqiliang/vector_bench_clickzetta:${VERSION} \
        -t czqiliang/vector_bench_clickzetta:latest \
		--pull=false \
		--push \
        .

build-docker-local-only:
	docker build -t czqiliang/vector_bench_clickzetta:${VERSION} .
	docker image tag czqiliang/vector_bench_clickzetta:${VERSION} czqiliang/vector_bench_clickzetta:latest



pull-docker:
	docker image pull czqiliang/vector_bench_clickzetta:latest

push-docker: ## Push multi-architecture Docker images
	@echo "Images are pushed during the build-docker step with --push."

unittest:
	PYTHONPATH=`pwd` python3 -m pytest tests/test_dataset.py::TestDataSet::test_download_small -svv

format:
	PYTHONPATH=`pwd` python3 -m black vectordb_bench
	PYTHONPATH=`pwd` python3 -m ruff check vectordb_bench --fix

lint:
	PYTHONPATH=`pwd` python3 -m black vectordb_bench --check
	PYTHONPATH=`pwd` python3 -m ruff check vectordb_bench

