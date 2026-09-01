FROM alpine:3.21
RUN apk --no-cache add ca-certificates tzdata \
    && adduser -D -u 10001 otel
COPY otelcol-contrib /otelcol-contrib
USER 10001
EXPOSE 4317 4318 11800 12800
ENTRYPOINT ["/otelcol-contrib"]
CMD ["--config", "/etc/otelcol-contrib/config.yaml"]
