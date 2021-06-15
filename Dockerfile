FROM golang:1.16.5-alpine3.13
MAINTAINER Gary Hetzel <its@gary.cool>

ENV GO111MODULE on
RUN apk update && apk add --no-cache bash gcc g++ ca-certificates curl wget make socat git jq
RUN go get github.com/ghetzel/pivot/cmd/pivot@v3.4.6
RUN rm -rf /go/pkg /go/src
RUN mv /go/bin/pivot /usr/bin/pivot
RUN rm -rf /usr/local/go /usr/libexec/gcc

RUN test -x /usr/bin/pivot

EXPOSE 29029
ENTRYPOINT ["/usr/bin/pivot"]
