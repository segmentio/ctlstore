FROM golang:1.20-alpine
ENV SRC github.com/segmentio/ctlstore
ARG VERSION

RUN apk --update add gcc git curl alpine-sdk libc6-compat ca-certificates sqlite \
  && curl -SsL https://github.com/segmentio/chamber/releases/download/v2.13.2/chamber-v2.13.2-linux-amd64 -o /bin/chamber \
  && curl -sL https://github.com/peak/s5cmd/releases/download/v2.1.0/s5cmd_2.1.0_Linux-64bit.tar.gz -o s5cmd.gz && tar -xzf s5cmd.gz -C /bin \
  && chmod +x /bin/chamber \
  && chmod +x /bin/s5cmd


COPY . /go/src/${SRC}
WORKDIR /go/src/${SRC}
RUN go mod vendor
RUN CGO_ENABLED=1 go install -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore \
  && cp ${GOPATH}/bin/ctlstore /usr/local/bin

RUN CGO_ENABLED=1 go install -ldflags="-X github.com/segmentio/ctlstore/pkg/version.version=$VERSION" ${SRC}/pkg/cmd/ctlstore-cli \
  && cp ${GOPATH}/bin/ctlstore-cli /usr/local/bin

FROM alpine
RUN apk --no-cache add sqlite pigz aws-cli perl-utils jq

COPY --from=0 /go/src/github.com/segmentio/ctlstore/scripts/download.sh .
COPY --from=0 /bin/chamber /bin/chamber
COPY --from=0 /bin/s5cmd /bin/s5cmd
COPY --from=0 /usr/local/bin/ctlstore /usr/local/bin/
COPY --from=0 /usr/local/bin/ctlstore-cli /usr/local/bin/
