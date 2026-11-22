FROM golang:alpine as build_container
WORKDIR /app
COPY ./gravity/go.mod ./gravity/go.sum ./
COPY ./magnetar ../magnetar
COPY ./oort ../oort
COPY ./meridian ../meridian
RUN go mod download
COPY ./gravity/ .
RUN go build -o agent_queue ./cmd/server/main.go

FROM alpine
COPY --from=build_container /app/agent_queue /usr/bin
ARG GRPC_PORT
EXPOSE $GRPC_PORT
ENTRYPOINT ["agent_queue"]
