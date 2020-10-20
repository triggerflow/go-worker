package eventsource

import (
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go"
)

var Constructors = map[string]func(string, chan *cloudevents.Event, json.RawMessage) EventSource{
	"KafkaEventSource": CreateKafkaEventSourceMappedConfig,
	"RedisEventSource": CreateRedisEventSourceMappedConfig,
	"RabbitMQEventSource": CreateRabbitMQEventSourceMappedConfig,
	"SQSEventSource": CreateSQSEventSourceMappedConfig,
}

type EventSource interface {
	StartConsuming()
	CommitEvents()
	Stop()
}
