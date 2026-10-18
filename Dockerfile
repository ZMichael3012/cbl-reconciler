# Build context: the services/ directory (needed for the replace directives on ../cbl-kit and ../cbl-protos).
# Build: docker build -f cbl-reconciler/Dockerfile -t cbl/reconciler:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY cbl-kit/ /src/cbl-kit/
COPY cbl-protos/ /src/cbl-protos/
COPY cbl-reconciler/ /src/cbl-reconciler/
WORKDIR /src/cbl-reconciler
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/reconciler ./cmd/reconciler

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/reconciler /app/reconciler
COPY --from=build /src/cbl-reconciler/migrations /app/migrations
EXPOSE 8100 9100 9300
ENTRYPOINT ["/app/reconciler"]
