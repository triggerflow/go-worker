package eventsource

import (
	"context"
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"sync"
	"time"
)

type KafkaEventSource struct {
	kafkaReader *kafka.Reader
	kafkaConn   *kafka.Conn
	eventSink   chan cloudevents.Event
	records     []kafka.Message
	recordsLock sync.Mutex
}

func CreateKafkaEventSource(workspace string, eventSink chan cloudevents.Event,
	topic string, bootstrapBrokers []string) EventSource {

	conn, err := kafka.DialLeader(context.Background(), "tcp", bootstrapBrokers[0], topic, 0)
	if err != nil {
		panic(err)
	}

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}
	err = conn.CreateTopics(topicConfig)
	if err != nil {
		panic(err)
	}

	kafkaEventSource := &KafkaEventSource{
		kafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  bootstrapBrokers,
			GroupID:  workspace,
			Topic:    topic,
			MinBytes: 1,    // 1 B
			MaxBytes: 10e6, // 10MB
		}),
		kafkaConn:   conn,
		eventSink:   eventSink,
		records:     make([]kafka.Message, 0),
		recordsLock: sync.Mutex{},
	}

	return kafkaEventSource
}

func CreateKafkaEventSourceMappedConfig(workspace string, eventSink chan cloudevents.Event,
	config json.RawMessage) EventSource {

	JSONconfig := make(map[string]interface{})
	err := json.Unmarshal(config, &JSONconfig)
	if err != nil {
		panic(err)
	}
	topic := JSONconfig["topic"].(string)
	auxBootstrapBrokers := JSONconfig["broker_list"].([]interface{})
	bootstrapBrokers := make([]string, len(auxBootstrapBrokers))
	for i, bootstrapBroker := range auxBootstrapBrokers {
		bootstrapBrokers[i] = bootstrapBroker.(string)
	}

	return CreateKafkaEventSource(workspace, eventSink, topic, bootstrapBrokers)
}

func (kafkaEs *KafkaEventSource) StartConsuming() {
	first := true

	for {
		m, err := kafkaEs.kafkaReader.FetchMessage(context.Background())
		if err != nil {
			panic(err)
		}

		if first {
			fmt.Println(time.Now().UTC().UnixNano())
			first = false
		}

		go func(message kafka.Message) {
			cloudevent := cloudevents.NewEvent()
			cloudevent.SetSpecVersion(fastjson.GetString(message.Value, "specversion"))
			cloudevent.SetID(fastjson.GetString(message.Value, "id"))
			cloudevent.SetType(fastjson.GetString(message.Value, "type"))
			cloudevent.SetSubject(fastjson.GetString(message.Value, "subject"))
			cloudevent.SetSource(fastjson.GetString(message.Value, "source"))
			dataContentType := fastjson.GetString(message.Value, "datacontenttype")
			if dataContentType != "" {
				if dataContentType == "application/json" {
					data := make(map[string]interface{})
					err := json.Unmarshal(fastjson.GetBytes(message.Value, "data"), &data)
					if err != nil {
						log.Warnf("[RedisEventSource] Could not decode data from cloudevent: %s", err)
					} else {
						cloudevent.SetDataContentType(dataContentType)
						_ = cloudevent.SetData(data)
					}
				} else {
					cloudevent.SetDataContentType(dataContentType)
					_ = cloudevent.SetData(fastjson.GetBytes(message.Value, "data"))
				}
			}

			//kafkaEs.recordsLock.Lock()
			kafkaEs.eventSink <- cloudevent
			//kafkaEs.records = append(kafkaEs.records, message)
			//kafkaEs.recordsLock.Unlock()
		}(m)
	}
}

func (kafkaEs *KafkaEventSource) Pause() {
	panic("implement me")
}

func (kafkaEs *KafkaEventSource) Resume() {
	panic("implement me")
}

func (kafkaEs *KafkaEventSource) CommitEvents() {
	ctx := context.Background()
	log.Infof("[KafkaEventSource] Going to commit %d messages", len(kafkaEs.records))
	err := kafkaEs.kafkaReader.CommitMessages(ctx, kafkaEs.records...)
	if err != nil {
		panic(err)
	}
	kafkaEs.records = make([]kafka.Message, 0)
	log.Infof("[KafkaEventSource] Ok -- message buffer empty")
}
