FROM --platform=linux/amd64 golang:1.14-alpine
ENV SRC github.com/segmentio/ctlstore
ARG VERSION

RUN apk --update add gcc git curl alpine-sdk libc6-compat ca-certificates sqlite \
  && curl -SsL https://github.com/segmentio/chamber/releases/download/v2.1.0/chamber-v2.1.0-linux-amd64 -o /bin/chamber \
  && chmod +x /bin/chamber

COPY . /go/src/${SRC}

RUN GO111MODULE=on go get github.com/go-delve/delve/cmd/dlv@latest

RUN CGO_ENABLED=1 go install -gcflags "all=-N -l" -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore \
  && cp ${GOPATH}/bin/ctlstore /usr/local/bin

RUN CGO_ENABLED=1 go install -gcflags "all=-N -l" -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore-cli \
  && cp ${GOPATH}/bin/ctlstore-cli /usr/local/bin

RUN apk del gcc git curl alpine-sdk libc6-compat

FROM --platform=linux/amd64 528451384384.dkr.ecr.us-west-2.amazonaws.com/segment-alpine
RUN apk --no-cache add sqlite

COPY --from=0 /bin/chamber /bin/chamber
COPY --from=0 /usr/local/bin/ctlstore /usr/local/bin/
COPY --from=0 /usr/local/bin/ctlstore-cli /usr/local/bin/
COPY --from=0 /go/bin/dlv /bin/dlv
