# syntax=docker/dockerfile:1.6
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/playground ./examples/playground/cmd/playground

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/playground /app/playground
EXPOSE 8080 9090
ENTRYPOINT ["/app/playground"]
