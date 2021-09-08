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

	levels := []logrus.Level{}
	for _, l := range []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	} {
		if l <= level {
			levels = append(levels, l)
		}
	}

	ctx, cancel := context.WithCancel(context.TODO())

	res, err := client.Indices.CreateDataStream(indexFunc())
	if err != nil { return nil, fmt.Errorf("failed to create new datastream: %v", err) }
	if res.IsError() { return nil, fmt.Errorf("failed to create new datastream: %v", res.String()) }

	processor, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:  			client,
		// flush if buffer crosses 50MB
		FlushBytes:  		1024 * 1024 * 50,
		// flush if buffer has existed for more than 30 seconds
		FlushInterval:  	time.Second * 30,

		// handle errors by logging to stderr
		OnError: 			func(ctx context.Context, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[%v] ERROR: failed to flush elasticsearch log buffer - %v\n", time.Now(), err)
		},
	})
	if err != nil { return nil, fmt.Errorf("failed to create bulk indexer: %v", err) }

	return &ElasticSearchHook{
		processor: processor,
		client:    client,
		index:     indexFunc,
		host:      host,
		levels:    levels,
		ctx:       ctx,
		ctxCancel: cancel,
	}, nil
}

func (hook *ElasticSearchHook) Fire(entry *logrus.Entry) error {
	data := map[string]interface{}{
		"Host":       hook.host,
		"@timestamp": entry.Time.UTC().Format(time.RFC3339Nano),
		"Message":    entry.Message,
		"Level":      strings.ToUpper(entry.Level.String()),
	}

	for k, v := range entry.Data {
		data[k] = v
	}

	if e, ok := data[logrus.ErrorKey]; ok && e != nil {
		if err, ok := e.(error); ok {
			data[logrus.ErrorKey] = err.Error()
		}
	}

	byteData, err := json.Marshal(data)
	if err != nil { return fmt.Errorf("failed to marshall json log: %v", err) }

	err = hook.processor.Add(context.TODO(), esutil.BulkIndexerItem{
		Index: 			hook.index(),
		Action: 		"create",
		Body: 			bytes.NewReader(byteData),
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
