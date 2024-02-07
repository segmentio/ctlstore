VERSION     := $(shell git describe --tags --always --dirty="-dev")
LDFLAGS     := -ldflags='-X "github.com/segmentio/ctlstore/pkg/version.version=$(VERSION)"'
DOCKER_REPO := 528451384384.dkr.ecr.us-west-2.amazonaws.com/ctlstore
Q=

GOTESTFLAGS = -tags="sqlite_preupdate_hook" -race -count 1

export GO111MODULE?=on

.PHONY: deps
deps:
	$Qgo get -d ./...

.PHONY: vendor
vendor:
	$Qgo mod vendor

.PHONY: clean
clean:
	$Qrm -rf vendor/ && git checkout ./vendor && dep ensure

.PHONY: install
install:
	$Qgo install -tags="sqlite_preupdate_hook" ./pkg/cmd/ctlstore

.PHONY: build
build: deps
	$Qgo build -tags="sqlite_preupdate_hook" -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=${VERSION} -X github.com/segmentio/ctlstore/pkg/globalstats.version=${VERSION}" -o ./bin/ctlstore ./pkg/cmd/ctlstore

.PHONY: docker
docker:
	$Qdocker build --build-arg VERSION=$(VERSION) \
		-t $(DOCKER_REPO):$(VERSION) \
		.

.PHONY: releasecheck
releasecheck:
	$Qexit $(shell git status --short | wc -l)

.PHONY: release-nonmaster
release-nonmaster: docker
	$Qdocker push $(DOCKER_REPO):$(VERSION)

.PHONY: release
release: docker
	$Qdocker tag $(DOCKER_REPO):$(VERSION) $(DOCKER_REPO):latest
	$Qdocker push $(DOCKER_REPO):$(VERSION)
	$Qdocker push $(DOCKER_REPO):latest

.PHONY: release-stable
release-stable: docker
	$Qdocker tag $(DOCKER_REPO):$(VERSION) $(DOCKER_REPO):stable
	$Qdocker push $(DOCKER_REPO):stable

.PHONY: vet
vet:
	$Qgo vet -tags=sqlite3_preupdate_hook ./...

.PHONY: generate
generate:
	$Qgo generate -tags=sqlite3_preupdate_hook ./...

.PHONY: fmtcheck
fmtchk:
	$Qexit $(shell gofmt -l . | grep -v '^vendor' | wc -l)

.PHONY: fmtfix
fmtfix:
	$Qgofmt -w $(shell find . -iname '*.go' | grep -v vendor)

.PHONY: test
test:
	$Qgo test $(GOTESTFLAGS) ./...

.PHONY: bench
bench:
	$Qgo test $(GOTESTFLAGS) -bench .
