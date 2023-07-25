FROM golang:1.20-alpine
ENV SRC github.com/segmentio/ctlstore
ARG VERSION

RUN apk --update add gcc git curl alpine-sdk libc6-compat ca-certificates sqlite \
  && curl -SsL https://github.com/segmentio/chamber/releases/download/v2.13.2/chamber-v2.13.2-linux-amd64 -o /bin/chamber \
  && chmod +x /bin/chamber

COPY . /go/src/${SRC}
WORKDIR /go/src/${SRC}
ENV GO111MODULE=on

RUN go mod vendor
RUN CGO_ENABLED=1 go install -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore \
  && cp ${GOPATH}/bin/ctlstore /usr/local/bin

RUN CGO_ENABLED=1 go install -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore-cli \
  && cp ${GOPATH}/bin/ctlstore-cli /usr/local/bin

RUN apk del gcc git curl alpine-sdk libc6-compat

FROM alpine
RUN apk --no-cache add sqlite

COPY --from=0 /bin/chamber /bin/chamber
COPY --from=0 /usr/local/bin/ctlstore /usr/local/bin/
COPY --from=0 /usr/local/bin/ctlstore-cli /usr/local/bin/
