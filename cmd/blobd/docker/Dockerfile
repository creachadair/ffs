FROM golang:1.17-alpine AS build
ARG TAGS
RUN apk add build-base git
RUN go install -tags ${TAGS} github.com/creachadair/ffs/cmd/blobd@latest

FROM alpine:latest AS run
VOLUME /data
WORKDIR /data
COPY --from=build /go/bin/blobd /usr/local/bin
ENTRYPOINT ["/usr/local/bin/blobd"]
