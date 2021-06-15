FROM golang:1.16.5-alpine3.13

ENV GO111MODULE on
ENV LOGLEVEL    debug

RUN apk update && apk add --no-cache bash gcc g++ ca-certificates curl wget make socat git jq libsass-dev libsass
RUN go get github.com/ghetzel/pivot/v3/cmd/pivot@v3.4.6
RUN rm -rf /go/pkg /go/src
RUN mv /go/bin/pivot /usr/bin/pivot
RUN rm -rf /usr/local/go /usr/libexec/gcc
RUN mkdir /config
RUN echo '---' > /config/pivot.yml

RUN test -x /usr/bin/pivot

EXPOSE 29029
ENTRYPOINT ["/usr/bin/pivot", "--config", "/config/pivot.yml", "web"]
