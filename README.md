# prometheus-influxdb-adapter

Prometheus remote storage adapter for InfluxDB.

## Usage

    ./prometheus-influxdb-adapter

### Flags

    $ ./prometheus-influxdb-adapter --help
    Usage of ./prometheus-influxdb-adapter:
      -log.level string
            Only log messages with the given severity or above. One of: [debug, info, warn, error] (default "info")
      -read.influxdb.database string
            InfluxDB database to read points from. (default "prometheus")
      -read.influxdb.field value
            Field to read sample values from.
      -read.influxdb.retention-policy value
            Retention policy to query from.
      -read.influxdb.rpc-address string
            Address of InfluxDB RPC server to receive points from. (default ":8082")
      -version
            Print version information.
      -web.listen-address string
            Address to listen on for web interface and telemetry. (default ":9201")
      -web.telemetry-path string
            Path under which to expose metrics. (default "/metrics")
      -write.influxdb.consistency-level string
            Default consistency level of the write operation.
      -write.influxdb.database string
            InfluxDB database to write points into. (default "prometheus")
      -write.influxdb.field string
            Field to store sample values in InfluxDB. (default "value")
      -write.influxdb.retention-policy string
            Default retention policy to use for points written to InfluxDB.
      -write.influxdb.timeout duration
            Timeout for sending points to InfluxDB. (default 30s)
      -write.influxdb.url string
            URL of InfluxDB server to send points to. (default "http://localhost:8086")

### Docker

You can deploy this adapter using the [kaorimatz/prometheus-influxdb-adapter](https://hub.docker.com/r/kaorimatz/prometheus-influxdb-adapter/) Docker image.

    docker run -d -p 9201:9201 kaorimatz/prometheus-influxdb-adapter

## Development

### Building

    make

### Building Docker image

    make docker
