package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/activation"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/yamux"
	"github.com/influxdata/yarpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/prompb"
)

const (
	defaultField        = "value"
	fieldTagKey         = "_field"
	measurementTagKey   = "_measurement"
	prometheusNamespace = "prometheus_influxdb_adapter"
)

var (
	readInfluxdbDatabase = flag.String(
		"read.influxdb.database",
		"prometheus",
		"InfluxDB database to read points from.",
	)
	readInfluxdbRPCAddress = flag.String(
		"read.influxdb.rpc-address",
		":8082",
		"Address of InfluxDB RPC server to receive points from.",
	)
	writeInfluxdbConsistencyLevel = flag.String(
		"write.influxdb.consistency-level",
		"",
		"Default consistency level of the write operation.",
	)
	writeInfluxdbDatabase = flag.String(
		"write.influxdb.database",
		"prometheus",
		"InfluxDB database to write points into.",
	)
	writeInfluxdbField = flag.String(
		"write.influxdb.field",
		defaultField,
		"Field to store sample values in InfluxDB.",
	)
	writeInfluxdbRetentionPolicy = flag.String(
		"write.influxdb.retention-policy",
		"",
		"Default retention policy to use for points written to InfluxDB.",
	)
	writeInfluxdbURL = flag.String(
		"write.influxdb.url",
		"",
		"URL of InfluxDB server to send points to.",
	)
	writeInfluxdbTimeout = flag.Duration(
		"write.influxdb.timeout",
		30*time.Second,
		"Timeout for sending points to InfluxDB.",
	)
	logLevel = flag.String(
		promlogflag.LevelFlagName,
		"info",
		promlogflag.LevelFlagHelp,
	)
	printVersion = flag.Bool(
		"version",
		false,
		"Print version information.",
	)
	listenAddress = flag.String(
		"web.listen-address",
		":9201",
		"Address to listen on for web interface and telemetry.",
	)
	telemetryPath = flag.String(
		"web.telemetry-path",
		"/metrics",
		"Path under which to expose metrics.",
	)

	readInfluxdbFieldValue              fieldValue
	readInfluxdbRetentionPolicySelector retentionPolicyValue
)

var (
	droppedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: prometheusNamespace,
			Name:      "dropped_samples_total",
			Help:      "Total number of dropped samples.",
		},
	)
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: prometheusNamespace,
			Name:      "received_samples_total",
			Help:      "Total number of received samples.",
		},
	)

	readPoints = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: prometheusNamespace,
			Name:      "read_points_total",
			Help:      "Total number of points read from InfluxDB.",
		},
		[]string{"database", "retention_policy"},
	)
	writtenPoints = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: prometheusNamespace,
			Name:      "written_points_total",
			Help:      "Total number of points written to InfluxDB.",
		},
		[]string{"database", "retention_policy"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: prometheusNamespace,
			Name:      "http_request_duration_seconds",
			Help:      "Histogram of HTTP request latencies.",
			Buckets:   []float64{.1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"handler"},
	)
	requestSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: prometheusNamespace,
			Name:      "http_request_size_bytes",
			Help:      "Histogram of HTTP request size.",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 8),
		},
		[]string{"handler"},
	)
	requests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: prometheusNamespace,
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests.",
		},
		[]string{"handler", "code"},
	)
	responseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: prometheusNamespace,
			Name:      "http_response_size_bytes",
			Help:      "Histogram of HTTP response size.",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 8),
		},
		[]string{"handler"},
	)
)

type fieldValue struct {
	fallback string
	fields   map[string]string
}

func (v *fieldValue) String() string {
	var b strings.Builder
	for fun, field := range v.fields {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(fun)
		b.WriteByte(':')
		b.WriteString(field)
	}
	if v.fallback != "" {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(v.fallback)
	}
	return b.String()
}

func (v *fieldValue) Set(value string) error {
	kv := strings.SplitN(value, ":", 2)
	if len(kv) == 2 {
		fun, field := kv[0], kv[1]
		if _, ok := v.fields[fun]; ok {
			return fmt.Errorf("multiple fields are set for %s", field)
		}
		if v.fields == nil {
			v.fields = make(map[string]string)
		}
		v.fields[fun] = field
	} else if v.fallback != "" {
		return fmt.Errorf("multiple fallback fields are set")
	} else {
		v.fallback = value
	}
	return nil
}

func (v *fieldValue) selector() (*fieldSelector, error) {
	if v.fallback == "" && v.fields == nil {
		return &fieldSelector{fallback: defaultField}, nil
	} else if v.fallback == "" {
		return nil, errors.New("fallback field must be set")
	} else {
		return &fieldSelector{fallback: v.fallback, fields: v.fields}, nil
	}
}

type fieldSelector struct {
	fallback string
	fields   map[string]string
}

func (s *fieldSelector) get(f string) string {
	if field, ok := s.fields[f]; ok {
		return field
	}
	return s.fallback
}

type retentionPolicyValue struct {
	fallback string
	policies map[int64]string
}

func (v *retentionPolicyValue) String() string {
	var b strings.Builder
	for step, policy := range v.policies {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(time.Duration(step).String())
		b.WriteByte(':')
		b.WriteString(policy)
	}
	if v.fallback != "" {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(v.fallback)
	}
	return b.String()
}

func (v *retentionPolicyValue) Set(value string) error {
	kv := strings.SplitN(value, ":", 2)
	if len(kv) == 2 {
		s, policy := kv[0], kv[1]

		d, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		step := int64(d)

		if _, ok := v.policies[step]; ok {
			return fmt.Errorf("multiple retention policies are set for %s", time.Duration(step).String())
		}
		if v.policies == nil {
			v.policies = make(map[int64]string)
		}
		v.policies[step] = policy
	} else if v.fallback != "" {
		return fmt.Errorf("multiple fallback retention policies are set")
	} else {
		v.fallback = value
	}
	return nil
}

func (v *retentionPolicyValue) selector() *retentionPolicySelector {
	steps := make([]int64, 0, len(v.policies))
	for step, _ := range v.policies {
		steps = append(steps, step)
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i] > steps[j] })

	policies := make([]string, len(v.policies))
	for i, step := range steps {
		policies[i] = v.policies[step]
	}

	return &retentionPolicySelector{fallback: v.fallback, policies: policies, steps: steps}
}

type retentionPolicySelector struct {
	fallback string
	policies []string
	steps    []int64
}

func (s *retentionPolicySelector) get(step int64) string {
	for i, st := range s.steps {
		if st <= step {
			return s.policies[i]
		}
	}
	return s.fallback
}

func init() {
	flag.Var(&readInfluxdbFieldValue, "read.influxdb.field", "Field to read sample values from.")
	flag.Var(&readInfluxdbRetentionPolicySelector, "read.influxdb.retention-policy", "Retention policy to query from.")

	flag.Lookup("read.influxdb.field").DefValue = defaultField
	flag.Lookup("write.influxdb.url").DefValue = "http://localhost:8086"

	prometheus.MustRegister(droppedSamples)
	prometheus.MustRegister(readPoints)
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(requestSize)
	prometheus.MustRegister(requests)
	prometheus.MustRegister(responseSize)
	prometheus.MustRegister(writtenPoints)
	prometheus.MustRegister(version.NewCollector(prometheusNamespace))
}

func main() {
	flag.Parse()

	if *printVersion {
		fmt.Println(version.Print("prometheus-influxdb-adapter"))
		return
	}

	allowedLevel := promlog.AllowedLevel{}
	allowedLevel.Set(*logLevel)
	logger := promlog.New(allowedLevel)

	level.Info(logger).Log("msg", "Starting prometheus-influxdb-adapter", "info", version.Info())
	level.Info(logger).Log("build_context", version.BuildContext())

	if f := flag.Lookup("write.influxdb.url"); f.Value.String() == "" {
		if u := os.Getenv("WRITE_INFLUXDB_URL"); len(u) > 0 {
			*writeInfluxdbURL = u
		} else {
			*writeInfluxdbURL = f.DefValue
		}
	}

	u, err := url.Parse(*writeInfluxdbURL)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to parse InfluxDB URL", "url", *writeInfluxdbURL, "err", err)
		os.Exit(1)
	}

	rc := &readerConfig{
		influxdbDatabase:   *readInfluxdbDatabase,
		influxdbRPCAddress: *readInfluxdbRPCAddress,
	}
	fieldSelector, err := readInfluxdbFieldValue.selector()
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	retentionPolicySelector := readInfluxdbRetentionPolicySelector.selector()
	reader, err := newReader(logger, rc, fieldSelector, retentionPolicySelector)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	wc := &writerConfig{
		influxdbConsistencyLevel: *writeInfluxdbConsistencyLevel,
		influxdbDatabase:         *writeInfluxdbDatabase,
		influxdbField:            *writeInfluxdbField,
		influxdbURL:              u,
		influxdbTimeout:          *writeInfluxdbTimeout,
	}
	writer, err := newWriter(logger, wc)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/read", instrumentHandler("/read", newReadHandler(logger, reader)))
	mux.Handle("/write", instrumentHandler("/write", newWriteHandler(logger, writer)))
	mux.Handle(*telemetryPath, promhttp.Handler())

	server := &http.Server{Addr: *listenAddress, Handler: mux}

	listeners, err := activation.Listeners()
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	if len(listeners) > 2 {
		level.Error(logger).Log("msg", "Too many file descriptors passed")
	}

	errCh := make(chan error)
	go func() {
		if len(listeners) > 0 {
			errCh <- server.Serve(listeners[0])
		} else {
			level.Info(logger).Log("msg", "Listening on "+*listenAddress)
			errCh <- server.ListenAndServe()
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		level.Error(logger).Log("err", err)
		os.Exit(1)
	case signal := <-signalCh:
		level.Info(logger).Log("msg", "Received a shutdown signal", "signal", signal)
		if err := server.Shutdown(context.Background()); err != nil {
			level.Error(logger).Log("err", err)
			os.Exit(1)
		}
	}
}

func instrumentHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	return promhttp.InstrumentHandlerCounter(
		requests.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerDuration(
			requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			promhttp.InstrumentHandlerRequestSize(
				requestSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
				promhttp.InstrumentHandlerResponseSize(
					responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
					handler,
				),
			),
		),
	)
}

type readHandler struct {
	logger log.Logger
	reader *reader
}

func newReadHandler(logger log.Logger, reader *reader) *readHandler {
	return &readHandler{logger: logger, reader: reader}
}

func (h *readHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error reading request body", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error decoding request body", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		level.Error(h.logger).Log("msg", "Error unmarshalling request body", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	db := r.FormValue("db")
	rp := r.FormValue("rp")

	level.Debug(h.logger).Log("query", req.Queries[0])

	resp, err := h.reader.read(r.Context(), &req, db, rp)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error handling read request", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error marshaling read response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		level.Error(h.logger).Log("msg", "Error encoding read response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type readerConfig struct {
	influxdbDatabase   string
	influxdbRPCAddress string
}

type reader struct {
	client                  storage.StorageClient
	clientLock              sync.Mutex
	config                  *readerConfig
	fieldSelector           *fieldSelector
	logger                  log.Logger
	retentionPolicySelector *retentionPolicySelector
}

func newReader(logger log.Logger, config *readerConfig, fieldSelector *fieldSelector, retentionPolicySelector *retentionPolicySelector) (*reader, error) {
	level.Info(logger).Log("msg", "Connecting to InfluxDB RPC Server", "address", config.influxdbRPCAddress)
	conn, err := yarpc.Dial(config.influxdbRPCAddress)
	if err != nil {
		return nil, err
	}

	return &reader{
		client:                  storage.NewStorageClient(conn),
		config:                  config,
		fieldSelector:           fieldSelector,
		logger:                  logger,
		retentionPolicySelector: retentionPolicySelector,
	}, nil
}

func (r *reader) read(ctx context.Context, req *prompb.ReadRequest, db, rp string) (*prompb.ReadResponse, error) {
	sreq, err := r.readRequestToStorageReadRequest(req, db, rp)
	if err != nil {
		return nil, err
	}

	stream, err := r.client.Read(ctx, sreq)
	if err == yamux.ErrSessionShutdown {
		level.Info(r.logger).Log("msg", "Session was already closed.")
		stream, err = r.retryRead(ctx, sreq)
	}
	if err != nil {
		return nil, err
	}

	resp, n, err := receive(stream)
	if err != nil {
		return nil, err
	}

	db, rp = sreq.Database, ""
	if i := strings.IndexByte(db, '/'); i != -1 {
		db, rp = db[:i], db[i+1:]
	}
	readPoints.WithLabelValues(db, rp).Add(float64(n))

	return resp, nil
}

func (r *reader) retryRead(ctx context.Context, sreq *storage.ReadRequest) (storage.Storage_ReadClient, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()

	stream, err := r.client.Read(ctx, sreq)
	if err == yamux.ErrSessionShutdown {
		level.Info(r.logger).Log("msg", "Reconnecting to InfluxDB RPC Server", "address", r.config.influxdbRPCAddress)
		conn, err := yarpc.Dial(r.config.influxdbRPCAddress)
		if err != nil {
			return nil, err
		}

		r.client = storage.NewStorageClient(conn)
		return r.client.Read(ctx, sreq)
	}

	return stream, err
}

func (r *reader) readRequestToStorageReadRequest(req *prompb.ReadRequest, db, rp string) (*storage.ReadRequest, error) {
	if len(req.Queries) != 1 {
		return nil, errors.New("read endpoint currently supports only one query at a time")
	}
	q := req.Queries[0]

	return r.queryToReadRequest(q, db, rp)
}

func (r *reader) queryToReadRequest(q *prompb.Query, db, rp string) (*storage.ReadRequest, error) {
	var sreq storage.ReadRequest

	sreq.Database = db
	if sreq.Database == "" {
		sreq.Database = r.config.influxdbDatabase
	}

	if rp == "" {
		var step int64
		if q.Hints != nil {
			step = q.Hints.StepMs * int64(time.Millisecond)
		}
		rp = r.retentionPolicySelector.get(step)
	}
	if rp != "" {
		sreq.Database += "/" + rp
	}

	sreq.TimestampRange.Start = time.Unix(0, q.StartTimestampMs*int64(time.Millisecond)).UnixNano()
	sreq.TimestampRange.End = time.Unix(0, q.EndTimestampMs*int64(time.Millisecond)).UnixNano()

	var fieldName string
	if q.Hints != nil {
		fieldName = r.fieldSelector.get(q.Hints.Func)
	} else {
		fieldName = r.fieldSelector.get("")
	}

	pred, err := matchersToPredicate(q.Matchers, fieldName)
	if err != nil {
		return nil, err
	}
	sreq.Predicate = pred

	return &sreq, nil
}

func matchersToPredicate(matchers []*prompb.LabelMatcher, fieldName string) (*storage.Predicate, error) {
	nodes := make([]*storage.Node, 0, len(matchers)+1)
	for _, matcher := range matchers {
		node, err := matcherToNode(matcher)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	nodes = append(nodes, comparisonNode(fieldTagKey, fieldName, storage.ComparisonEqual))

	return &storage.Predicate{
		Root: &storage.Node{
			NodeType: storage.NodeTypeLogicalExpression,
			Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
			Children: nodes,
		},
	}, nil
}

func matcherToNode(matcher *prompb.LabelMatcher) (*storage.Node, error) {
	key := matcher.Name
	if key == model.MetricNameLabel {
		key = measurementTagKey
	}

	var op storage.Node_Comparison
	switch matcher.Type {
	case prompb.LabelMatcher_EQ:
		op = storage.ComparisonEqual
	case prompb.LabelMatcher_NEQ:
		op = storage.ComparisonNotEqual
	case prompb.LabelMatcher_RE:
		op = storage.ComparisonRegex
	case prompb.LabelMatcher_NRE:
		op = storage.ComparisonNotRegex
	default:
		return nil, fmt.Errorf("unknown label matcher type: %v", matcher.Type)
	}

	return comparisonNode(key, matcher.Value, op), nil
}

func comparisonNode(key, value string, op storage.Node_Comparison) *storage.Node {
	lhs := &storage.Node{
		NodeType: storage.NodeTypeTagRef,
		Value: &storage.Node_TagRefValue{
			TagRefValue: key,
		},
	}

	var rhs *storage.Node
	if op == storage.ComparisonEqual || op == storage.ComparisonNotEqual {
		rhs = &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_StringValue{StringValue: value},
		}
	} else {
		rhs = &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_RegexValue{RegexValue: value},
		}
	}

	return &storage.Node{
		NodeType: storage.NodeTypeComparisonExpression,
		Value:    &storage.Node_Comparison_{Comparison: op},
		Children: []*storage.Node{lhs, rhs},
	}
}

func receive(stream storage.Storage_ReadClient) (*prompb.ReadResponse, int, error) {
	var result prompb.QueryResult
	var ts *prompb.TimeSeries
	var pointsTotal int

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, err
		}

		for _, frame := range resp.Frames {
			switch f := frame.GetData().(type) {
			case *storage.ReadResponse_Frame_Series:
				series := f.Series

				if series.DataType != storage.DataTypeFloat {
					return nil, 0, fmt.Errorf("unexpected frame type: %v", series.DataType)
				}

				labels := make([]*prompb.Label, 0, len(series.Tags)-1)
				for _, tag := range series.Tags {
					name := string(tag.Key)
					if name == fieldTagKey {
						continue
					}
					if name == measurementTagKey {
						name = model.MetricNameLabel
					}
					labels = append(labels, &prompb.Label{
						Name:  name,
						Value: string(tag.Value),
					})
				}

				if ts != nil {
					result.Timeseries = append(result.Timeseries, ts)
				}

				ts = &prompb.TimeSeries{Labels: labels}
			case *storage.ReadResponse_Frame_FloatPoints:
				points := f.FloatPoints

				pointsTotal += len(points.Timestamps)

				samples := make([]*prompb.Sample, 0, len(ts.Samples)+len(points.Timestamps))
				if ts.Samples != nil {
					copy(samples, ts.Samples)
				}

				for i, t := range points.Timestamps {
					samples = append(samples, &prompb.Sample{
						Value:     points.Values[i],
						Timestamp: t / int64(time.Millisecond),
					})
				}

				ts.Samples = samples
			default:
				return nil, 0, fmt.Errorf("unexpected frame data type: %T", frame.Data)
			}
		}
	}

	if ts != nil {
		result.Timeseries = append(result.Timeseries, ts)
	}

	return &prompb.ReadResponse{Results: []*prompb.QueryResult{&result}}, pointsTotal, nil
}

type writeHandler struct {
	logger log.Logger
	writer *writer
}

func newWriteHandler(logger log.Logger, writer *writer) *writeHandler {
	return &writeHandler{logger: logger, writer: writer}
}

func (h *writeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error reading request body", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error decoding request body", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		level.Error(h.logger).Log("msg", "Error unmarshalling request body", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	db := r.URL.Query().Get("db")
	rp := r.URL.Query().Get("rp")
	cl := r.URL.Query().Get("consistency")

	if err := h.writer.write(&req, db, rp, cl); err != nil {
		level.Error(h.logger).Log("msg", "Error handling write request", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type writerConfig struct {
	influxdbConsistencyLevel string
	influxdbDatabase         string
	influxdbField            string
	influxdbRetentionPolicy  string
	influxdbURL              *url.URL
	influxdbTimeout          time.Duration
}

type writer struct {
	client influx.Client
	config *writerConfig
	logger log.Logger
}

func newWriter(logger log.Logger, config *writerConfig) (*writer, error) {
	var client influx.Client
	var err error
	switch config.influxdbURL.Scheme {
	case "udp", "udp4", "udp6":
		addr := net.JoinHostPort(config.influxdbURL.Hostname(), config.influxdbURL.Port())
		client, err = influx.NewUDPClient(influx.UDPConfig{Addr: addr})
	case "http", "https":
		u := config.influxdbURL
		user := u.User
		u.User = nil
		c := influx.HTTPConfig{
			Addr:      u.String(),
			Username:  user.Username(),
			UserAgent: "prometheus-influxdb-adapter/" + version.Version,
			Timeout:   config.influxdbTimeout,
		}
		if password, ok := user.Password(); ok {
			c.Password = password
		}
		client, err = influx.NewHTTPClient(c)
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", config.influxdbURL.Scheme)
	}
	if err != nil {
		return nil, err
	}

	return &writer{client: client, config: config, logger: logger}, nil
}

func (w *writer) write(req *prompb.WriteRequest, db, rp, cl string) error {
	points, err := w.writeRequestToPoints(req)
	if err != nil {
		return err
	}

	if err := w.writePoints(points, db, rp, cl); err != nil {
		return err
	}

	return nil
}

func (w *writer) writeRequestToPoints(req *prompb.WriteRequest) ([]*influx.Point, error) {
	var n int
	for _, ts := range req.Timeseries {
		n += len(ts.Samples)
	}
	points := make([]*influx.Point, 0, n)

	receivedSamples.Add(float64(n))

	for _, ts := range req.Timeseries {
		var name string
		tags := make(map[string]string, len(ts.Labels)-1)
		for _, l := range ts.Labels {
			if l.Name == model.MetricNameLabel {
				name = l.Value
			} else {
				tags[l.Name] = l.Value
			}
		}

		for _, s := range ts.Samples {
			if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
				droppedSamples.Inc()
				continue
			}

			t := time.Unix(0, s.Timestamp*int64(time.Millisecond))
			fields := map[string]interface{}{w.config.influxdbField: s.Value}
			p, err := influx.NewPoint(name, tags, fields, t)
			if err != nil {
				return nil, err
			}

			points = append(points, p)
		}
	}

	return points, nil
}

func (w *writer) writePoints(points []*influx.Point, db, rp, cl string) error {
	c := influx.BatchPointsConfig{
		Precision:        "ms",
		Database:         db,
		RetentionPolicy:  rp,
		WriteConsistency: cl,
	}
	if c.Database == "" {
		c.Database = w.config.influxdbDatabase
	}
	if c.RetentionPolicy == "" {
		c.RetentionPolicy = w.config.influxdbRetentionPolicy
	}
	if c.WriteConsistency == "" {
		c.WriteConsistency = w.config.influxdbConsistencyLevel
	}

	bp, err := influx.NewBatchPoints(c)
	if err != nil {
		return err
	}

	bp.AddPoints(points)

	if err = w.client.Write(bp); err != nil {
		return err
	}

	writtenPoints.WithLabelValues(c.Database, c.RetentionPolicy).Add(float64(len(points)))

	return nil
}
