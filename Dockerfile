FROM golang:1.14-alpine
ENV SRC github.com/segmentio/ctlstore
ARG VERSION
ARG TARGETARCH

RUN apk --update add gcc git curl alpine-sdk libc6-compat ca-certificates sqlite

RUN apk --update add gcc git curl alpine-sdk libc6-compat ca-certificates sqlite \
  && curl -SsL https://github.com/segmentio/chamber/releases/download/v2.10.10/chamber-v2.10.10-linux-${TARGETARCH} -o /bin/chamber \
  && chmod +x /bin/chamber

COPY . /go/src/${SRC}

RUN CGO_ENABLED=1 GOARCH=${TARGETARCH} go install -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore \
  && cp ${GOPATH}/bin/ctlstore /usr/local/bin

RUN CGO_ENABLED=1 GOARCH=${TARGETARCH} go install -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore-cli \
  && cp ${GOPATH}/bin/ctlstore-cli /usr/local/bin

RUN apk del gcc git curl alpine-sdk libc6-compat

FROM alpine:3.14
RUN apk --no-cache add sqlite

COPY --from=0 /bin/chamber /bin/chamber
COPY --from=0 /usr/local/bin/ctlstore /usr/local/bin/
COPY --from=0 /usr/local/bin/ctlstore-cli /usr/local/bin/
