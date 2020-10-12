package eventsource

import (
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	log "github.com/sirupsen/logrus"
	rabbit "github.com/streadway/amqp"
	"sync"
	"time"
)

type RabbitMQEventSource struct {
	rabbitConn     *rabbit.Connection
	rabbitChan     *rabbit.Channel
	rabbitQueue    *rabbit.Queue
	workspace      string
	eventSink      chan *cloudevents.Event
	messageRecords sync.Map
}

func CreateRabbitMQEventSource(workspace string, eventSink chan *cloudevents.Event, queue string, amqpURL string) EventSource {

	conn, err := rabbit.Dial(amqpURL)
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	q, err := ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	rabbitEs := RabbitMQEventSource{
		rabbitConn:     conn,
		rabbitChan:     ch,
		rabbitQueue:    &q,
		workspace:      workspace,
		eventSink:      eventSink,
		messageRecords: sync.Map{},
	}

	return &rabbitEs
}

func CreateRabbitMQEventSourceMappedConfig(workspace string, eventSink chan *cloudevents.Event, config json.RawMessage) EventSource {
	JSONconfig := make(map[string]interface{})

	err := json.Unmarshal(config, &JSONconfig)
	if err != nil {
		panic(err)
	}

	queue := JSONconfig["queue"].(string)
	amqpURL := JSONconfig["amqp_url"].(string)

	return CreateRabbitMQEventSource(workspace, eventSink, queue, amqpURL)
}

func (rabbitEs *RabbitMQEventSource) StartConsuming() {
	log.Infof("Starting consuming from queue %s", rabbitEs.rabbitQueue.Name)

	msgs, err := rabbitEs.rabbitChan.Consume(
		rabbitEs.rabbitQueue.Name, // queue
		rabbitEs.workspace,        // consumer
		false,             // auto-ack
		false,            // exclusive
		false,             // no-local
		false,              // no-wait
		nil,                 // args
	)

	if err != nil {
		panic(err)
	}

	log.Infof("[RabbitMQEventSource] Consuming from queue %s", rabbitEs.rabbitQueue.Name)

	first := true
	for d := range msgs {
		if first {
			fmt.Println(time.Now().UTC().UnixNano())
			first = false
		}
		go func(message rabbit.Delivery) {
			cloudevent, err := DecodeCloudEventBytes(message.Body)
			if err != nil {
				panic(err)
			}

			rabbitEs.eventSink <- cloudevent
			rabbitEs.messageRecords.Store(cloudevent.Subject(), d.DeliveryTag)

		}(d)
	}
}

func (rabbitEs *RabbitMQEventSource) CommitEvents(subject string) {
	log.Infof("[RabbitMQEventSource] Going to commit events from subject %s", subject)

	value, ok := rabbitEs.messageRecords.Load(subject)
	if !ok {
		return
	}
	deliveryTag := value.(uint64)
	err := rabbitEs.rabbitChan.Ack(deliveryTag, true)
	if err != nil {
		panic(err)
	}
}

func (rabbitEs *RabbitMQEventSource) Stop() {
	err := rabbitEs.rabbitConn.Close()
	if err != nil {
		panic(err)
	}
	err = rabbitEs.rabbitConn.Close()
	if err != nil {
		panic(err)
	}
}
