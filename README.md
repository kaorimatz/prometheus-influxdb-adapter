# prometheus-influxdb-adapter

Prometheus remote storage adapter for InfluxDB.

## Usage

    ./prometheus-influxdb-adapter

### Read

Upon receiving a request from Prometheus, prometheus-influxdb-adapter sends the requet for reading samples to the
InfluxDB RPC server. The address of the RPC server (default is `:8082`) can be specified using
`--read.influxdb.rpc-address` flag.

    ./prometheus-influxdb-adapter --read.influxdb.rpc-address=influxdb.example.com:8082

Points are read from `prometheus` database and the default retention policy by default.

    ./prometheus-influxdb-adapter --read.influxdb.database db1 --read.influxdb.retention-policy=policy1

If you specify multiple retention policies prefixed with duration, prometheus-influxdb-adapter selects one based on the
duration and the interval step of the request. For example,

    ./prometheus-influxdb-adapter --read.influxdb.retention-policy=1h:1h --read.influxdb.retention-policy=5m:5m --read.influxdb.retention-policy=1m

`1h` retention policy is used if the interval step of the request is smaller than 1 hour, and `5m` retention policy is
used if the step is bigger than 5 minutes and smaller than 1 hour and `1m` is used if the step is smaller than 1 minute
or not given.

**Note**: The interval step in the request is available only if you are running the Prometheus server built from master
at the moment.

You can also change the field (default is `value` field) in InfluxDB to read the value from.

    ./prometheus-influxdb-adapter --read.influxdb.field=field1

If you prefix the field with a Prometheus's function name, the field is used only if the function name given in the
request matches the function name specified.

    ./prometheus-influxdb-adapter --read.influxdb.field=max_over_time:max --read.influxdb.field=min_over_time:min --read.influxdb.field=sum_over_time:sum --read.influxdb.field=mean

**Note**: The function name in the request is available only if you are running the Prometheus server built from
master at the moment.

### Write

By default, prometheus-influxdb-adapter sends points to http://localhost:8086. You can change it using the
`--write.influxdb.url` flag.

    ./prometheus-influxdb-adapter --write.influxdb.url=http://influxdb.example.com:8086

Points are written into `prometheus` database with the default retention policy by default.

    ./prometheus-influxdb-adapter --write.influxdb.database=db1 --wirte.influxdb.retention-policy=policy1

You can also change the field (default is `value` field) in InfluxDB for storing the value.

    ./prometheus-influxdb-adapter --write.influxdb.field=field1

### Flags

    $ ./prometheus-influxdb-adapter --help
    Usage of ./prometheus-influxdb-adapter:
      -log.level string
            Only log messages with the given severity or above. One of: [debug, info, warn, error] (default "info")
      -read.influxdb.database string
            InfluxDB database to read points from. (default "prometheus")
      -read.influxdb.field value
            Field to read sample values from. (default value)
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
