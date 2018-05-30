FROM quay.io/prometheus/golang-builder as builder

COPY . $GOPATH/src/github.com/kaorimatz/prometheus-influxdb-adapter
WORKDIR $GOPATH/src/github.com/kaorimatz/prometheus-influxdb-adapter

RUN make PREFIX=/

FROM quay.io/prometheus/busybox
MAINTAINER Satoshi Matsumoto <kaorimatz@gmail.com>

COPY --from=builder /prometheus-influxdb-adapter /bin/prometheus-influxdb-adapter

EXPOSE 9201
ENTRYPOINT ["prometheus-influxdb-adapter"]
