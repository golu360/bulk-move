# Go toolchain plus module cache. The test service runs from this stage.
FROM golang:1.25 AS dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build ./... && go vet ./...

FROM dev AS build
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

# Small runtime image; busybox wget serves the API healthcheck.
FROM alpine:3.20 AS runtime
COPY --from=build /out/api /out/worker /usr/local/bin/
EXPOSE 8080
CMD ["api"]
