FROM alpine
ENTRYPOINT ["/nats-pipe"]
ADD nats-pipe /
