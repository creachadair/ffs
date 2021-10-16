FROM golang:1.17-alpine AS build
RUN apk add build-base git
RUN go install \
       github.com/creachadair/ffs/cmd/blobd@latest

FROM alpine:latest AS run
ARG STORE=badger:blob-data.db
ARG PORT=19741
ENV STORE=$STORE
ENV ADDR=":$PORT"
VOLUME /data
WORKDIR /data
COPY --from=build /go/bin/blobd /usr/local/bin
EXPOSE ${PORT}/tcp
CMD ["sh", "-c", "blobd -store $STORE -listen $ADDR"]
