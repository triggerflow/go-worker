package eventsource

import (
	"context"
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
	"triggerflow/config"
)

type KafkaEventSource struct {
	kafkaReader *kafka.Reader
	kafkaConn   *kafka.Conn
	eventSink   chan *cloudevents.Event
	records     []kafka.Message
	recordsLock sync.Mutex
}

func CreateKafkaEventSource(workspace string, eventSink chan *cloudevents.Event,
	topic string, bootstrapBrokers []string) EventSource {

	conn, err := kafka.DialLeader(context.Background(), "tcp", bootstrapBrokers[0], topic, 0)
	if err != nil {
		panic(err)
	}

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
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

func CreateKafkaEventSourceMappedConfig(workspace string, eventSink chan *cloudevents.Event,
	config json.RawMessage) EventSource {

	JSONConfig := make(map[string]interface{})
	err := json.Unmarshal(config, &JSONConfig)
	if err != nil {
		panic(err)
	}
	topic := JSONConfig["topic"].(string)
	auxBootstrapBrokers := JSONConfig["broker_list"].([]interface{})
	bootstrapBrokers := make([]string, len(auxBootstrapBrokers))
	for i, bootstrapBroker := range auxBootstrapBrokers {
		bootstrapBrokers[i] = bootstrapBroker.(string)
	}

	return CreateKafkaEventSource(workspace, eventSink, topic, bootstrapBrokers)
}

func (kafkaEs *KafkaEventSource) StartConsuming() {
	recordsChan := make(chan kafka.Message, config.SinkMaxSize)

	go func(messageChannel chan kafka.Message) {
		for message := range messageChannel {
			kafkaEs.records = append(kafkaEs.records, message)
		}
	}(recordsChan)

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

		cloudevent, err := DecodeCloudEventBytes(m.Value)
		if err != nil {
			panic(err)
		}

		kafkaEs.eventSink <- cloudevent
		recordsChan <- m
	}
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

func (kafkaEs *KafkaEventSource) Stop() {
	panic("implement me")
}
