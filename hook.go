package elastic_logrus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"golang.org/x/net/context"
)

var levels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
	logrus.DebugLevel,
}

type IndexNameFunc func() string

type ElasticSearchHook struct {
	processor esutil.BulkIndexer
	client    *elasticsearch.Client
	host      string
	index     IndexNameFunc
	levels    []logrus.Level
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func NewElasticHook(
	client *elasticsearch.Client,
	host string,
	level logrus.Level,
	indexFunc IndexNameFunc,
	flushInterval time.Duration,
) (*ElasticSearchHook, error) {
	// create new slice to hold levels permitted by the logger
	loggerLevels := make([]logrus.Level, 0)
	// load all levels permitted by the logger into the slice
	for _, l := range levels {
		if l <= level { loggerLevels = append(loggerLevels, l) }
	}

	// create context for the hook
	ctx, cancel := context.WithCancel(context.TODO())

	// attempt to create the data stream
	res, err := client.Indices.CreateDataStream(indexFunc())

	// handle data stream creation failures excluding errors for existing data streams
	if err != nil && !strings.Contains(err.Error(), "\"type\":\"resource_already_exists_exception\"") {
		return nil, fmt.Errorf("failed to create new datastream: %v", err)
	}
	if res.IsError() && !strings.Contains(res.String(), "\"type\":\"resource_already_exists_exception\"") {
		return nil, fmt.Errorf("failed to create new datastream: %v", res.String())
	}

	// create a new bul processor
	processor, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:  			client,
		// flush if buffer crosses 50MB
		FlushBytes:  		1024 * 1024 * 50,
		// flush if buffer has existed for more than 30 seconds
		FlushInterval:  	flushInterval,

		// handle errors by logging to stderr
		OnError: 			func(ctx context.Context, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[%v] ERROR: failed to flush elasticsearch log buffer - %v\n", time.Now(), err)
		},
	})
	if err != nil { return nil, fmt.Errorf("failed to create bulk indexer: %v", err) }

	// assemble and return hook
	return &ElasticSearchHook{
		processor: processor,
		client:    client,
		index:     indexFunc,
		host:      host,
		levels:    loggerLevels,
		ctx:       ctx,
		ctxCancel: cancel,
	}, nil
}

func (hook *ElasticSearchHook) Fire(entry *logrus.Entry) error {
	// create base map containing necessary information for log
	data := map[string]interface{}{
		"Host":       hook.host,
		"@timestamp": entry.Time.UTC().Format(time.RFC3339Nano),
		"Message":    entry.Message,
		"Level":      strings.ToUpper(entry.Level.String()),
	}

	// add additional fields to log entry map
	for k, v := range entry.Data { data[k] = v }

	// handle errors added to the log
	if e, ok := data[logrus.ErrorKey]; ok && e != nil {
		if err, ok := e.(error); ok {
			data[logrus.ErrorKey] = err.Error()
		}
	}

	// marshall log data into json byte array
	byteData, err := json.Marshal(data)
	if err != nil { return fmt.Errorf("failed to marshall json log: %v", err) }

	// add log entry to the bulk processor
	err = hook.processor.Add(context.TODO(), esutil.BulkIndexerItem{
		// route the log entry to the index/data-stream relevant at this moment
		Index: 			hook.index(),
		// only create documents as we are append-only
		Action: 		"create",
		// format log data into reader and store as body
		Body: 			bytes.NewReader(byteData),
		// log failures for buffer appends to stderr
		OnFailure: 		func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error) {
			b, _ := ioutil.ReadAll(item.Body)
			if b == nil { b = make([]byte, 0) }
			_, _ = fmt.Fprintf(os.Stderr, "[%v] ERROR: failed to add elasticsearch log to buffer - %v\n    LOG: %s\n", time.Now(), err, string(b))
		},
	})
	if err != nil { return fmt.Errorf("failed to add json log to buffer: %v", err) }

	return nil
}

func (hook *ElasticSearchHook) Levels() []logrus.Level {
	return hook.levels
}

func (hook *ElasticSearchHook) Cancel() {
	hook.ctxCancel()
}

func (hook *ElasticSearchHook) Flush() {
	hook.processor.Close(context.TODO())
	hook.processor, _ = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:  			hook.client,
		// flush if buffer crosses 50MB
		FlushBytes:  		1024 * 1024 * 50,
		// flush if buffer has existed for more than 30 seconds
		FlushInterval:  	time.Second * 30,

		// handle errors by logging to stderr
		OnError: 			func(ctx context.Context, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[%v] ERROR: failed to flush elasticsearch log buffer - %v\n", time.Now(), err)
		},
	})
}
