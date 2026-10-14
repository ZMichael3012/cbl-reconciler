FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/reconciler ./cmd/reconciler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/reconciler /app/reconciler
EXPOSE 8100 9100 9300
ENTRYPOINT ["/app/reconciler"]