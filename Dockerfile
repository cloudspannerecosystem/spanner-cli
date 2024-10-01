# Build stage
FROM golang:1.23 as build
WORKDIR /go/src/spanner-cli
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -o /go/bin/spanner-cli

# Final stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /go/bin/spanner-cli /
ENTRYPOINT ["/spanner-cli"]
