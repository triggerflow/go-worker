package eventsource

import (
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"strconv"
	"sync"
	"time"
)

type RedisEventSource struct {
	client      *redis.Client
	workspace   string
	stream      string
	eventSink   chan *cloudevents.Event
	records     []string
	recordsLock sync.Mutex
}

func CreateRedisEventSource(workspace string, eventSink chan *cloudevents.Event,
	stream string, host string, port int, password string, db int) EventSource {
	var (
		statusCmd *redis.StatusCmd
		res       string
		err       error
	)

	client := redis.NewClient(&redis.Options{
		Addr:     host + ":" + strconv.Itoa(port),
		Password: password,
		DB:       db,
	})

	redisEventSource := &RedisEventSource{
		client:      client,
		eventSink:   eventSink,
		stream:      stream,
		workspace:   workspace,
		records:     make([]string, 0),
		recordsLock: sync.Mutex{},
	}

	statusCmd = redisEventSource.client.Ping()
	res, err = statusCmd.Result()
	if err != nil {
		panic(err)
	}
	log.Debugf("[RedisEventSource] %s", res)

	return redisEventSource
}

func CreateRedisEventSourceMappedConfig(workspace string, eventSink chan *cloudevents.Event,
	config json.RawMessage) EventSource {

	stream := fastjson.GetString(config, "stream")
	host := fastjson.GetString(config, "host")
	port := fastjson.GetInt(config, "port")
	password := fastjson.GetString(config, "password")
	db := fastjson.GetInt(config, "db")

	return CreateRedisEventSource(workspace, eventSink, stream, host, port, password, db)
}

func (redisEs *RedisEventSource) StartConsuming() {
	lastID := "0"
	log.Debugf("[RedisEventSource] Starting to consume from stream %s ...", redisEs.stream)
	for {
		events, err := redisEs.client.XRead(&(redis.XReadArgs{
			Streams: []string{redisEs.stream, lastID},
			Block:   0,
		})).Result()

		if err != nil {
			panic(err)
		}

		if lastID == "0" {
			fmt.Println(time.Now().UTC().UnixNano())
		}

		values := events[0].Messages
		lastID = values[len(values)-1].ID
		log.Debugf("[RedisEventSource] Pulled %d events", len(values))

		first := true
		for _, value := range values {
			if first {
				fmt.Println(time.Now().UTC().UnixNano())
				first = false
			}

			go func(rawEvent map[string]interface{}, ID string) {
				cloudevent := cloudevents.NewEvent()
				cloudevent.SetSpecVersion(rawEvent["specversion"].(string))
				cloudevent.SetID(rawEvent["id"].(string))
				cloudevent.SetType(rawEvent["type"].(string))
				cloudevent.SetSubject(rawEvent["subject"].(string))
				cloudevent.SetSource(rawEvent["source"].(string))
				if dataContentType, ok := rawEvent["datacontenttype"]; ok {
					err := cloudevent.SetData(rawEvent["data"])
					if err != nil {
						log.Warnf("[RedisEventSource] Could not decode data from cloudevent: %s", err)
					} else {
						cloudevent.SetDataContentType(dataContentType.(string))
					}
				}

				redisEs.eventSink <- &cloudevent
				redisEs.recordsLock.Lock()
				redisEs.records = append(redisEs.records, ID)
				redisEs.recordsLock.Unlock()

			}(value.Values, value.ID)
		}
	}
}

func (redisEs *RedisEventSource) CommitEvents() {
	log.Infof("[RedisEventSource] Going to commit %d events", len(redisEs.records))
	redisEs.client.XDel(redisEs.stream, redisEs.records...)
	redisEs.records = make([]string, 0)
}

func (redisEs *RedisEventSource) Stop() {
	panic("implement me")
}
