FROM golang:1.26.6-alpine3.23 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=development
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X github.com/dsub-io/go-open-discogs-batch/cmd.version=${VERSION}" \
    -o /out/go-open-discogs-batch \
    .
RUN mkdir -p /out/home/nonroot/.cache/open-discogs-batch && \
    chown -R 65532:65532 /out/home/nonroot

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/go-open-discogs-batch /go-open-discogs-batch
COPY --from=build --chown=65532:65532 /out/home/nonroot /home/nonroot

ENV HOME=/home/nonroot
USER 65532:65532
WORKDIR /home/nonroot
ENTRYPOINT ["/go-open-discogs-batch"]
