VERSION     := $(shell git describe --tags --always --dirty="-dev")
LDFLAGS     := -ldflags='-X "github.com/segmentio/ctlstore.Version=$(VERSION)"'
DOCKER_REPO := 528451384384.dkr.ecr.us-west-2.amazonaws.com/ctlstore
Q=

GOTESTFLAGS = -race -count 1

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
	$Qgo install ./pkg/cmd/ctlstore

.PHONY: build
build: deps
	$Qgo build -ldflags="-X github.com/segmentio/ctlstore.Version=${VERSION}" -o ./bin/ctlstore ./pkg/cmd/ctlstore

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
	$Qgo vet ./...

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

.PHONY: trebvet
trebvet:
	$Qecho "production ctlstore $(shell treb config validate .run/ctlstore-executive.yml -e production)"
	$Qecho "stage ctlstore $(shell treb config validate .run/ctlstore-executive.yml -e stage)"


.PHONY: gensite
gensite:
	$Qdocker build \
		-f Dockerfile-webgen \
		-t ctlstore-webgen \
		.
	$Qdocker run \
	    -it --rm \
		-v $(PWD)/website/gen:/out \
		-v $(PWD):/pwd \
		ctlstore-webgen

.PHONY: site
site: gensite install
	$(GOPATH)/bin/ctlstore site
