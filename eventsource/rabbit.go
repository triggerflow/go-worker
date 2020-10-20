package eventsource

import (
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go"
	log "github.com/sirupsen/logrus"
	rabbit "github.com/streadway/amqp"
	"sync/atomic"
)

type RabbitMQEventSource struct {
	rabbitConn      *rabbit.Connection
	rabbitChan      *rabbit.Channel
	rabbitQueue     *rabbit.Queue
	workspace       string
	eventSink       chan *cloudevents.Event
	lastDeliveryTag uint64
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
		rabbitConn:      conn,
		rabbitChan:      ch,
		rabbitQueue:     &q,
		workspace:       workspace,
		eventSink:       eventSink,
		lastDeliveryTag: 0,
	}

	return &rabbitEs
}

func CreateRabbitMQEventSourceMappedConfig(workspace string, eventSink chan *cloudevents.Event, config json.RawMessage) EventSource {
	JSONConfig := make(map[string]interface{})

	err := json.Unmarshal(config, &JSONConfig)
	if err != nil {
		panic(err)
	}

	queue := JSONConfig["queue"].(string)
	amqpURL := JSONConfig["amqp_url"].(string)

	return CreateRabbitMQEventSource(workspace, eventSink, queue, amqpURL)
}

func (rabbitEs *RabbitMQEventSource) StartConsuming() {
	log.Infof("Starting consuming from queue %s", rabbitEs.rabbitQueue.Name)

	msgs, err := rabbitEs.rabbitChan.Consume(
		rabbitEs.rabbitQueue.Name, // queue
		rabbitEs.workspace,        // consumer
		false,                     // auto-ack
		false,                     // exclusive
		false,                     // no-local
		false,                     // no-wait
		nil,                       // args
	)

	if err != nil {
		panic(err)
	}

	log.Infof("[RabbitMQEventSource] Consuming from queue %s", rabbitEs.rabbitQueue.Name)

	for msg := range msgs {
		go func(message *rabbit.Delivery) {
			cloudevent, err := DecodeCloudEventBytes(message.Body)
			if err != nil {
				panic(err)
			}

			rabbitEs.eventSink <- cloudevent
			atomic.StoreUint64(&rabbitEs.lastDeliveryTag, message.DeliveryTag)
		}(&msg)
	}
}

func (rabbitEs *RabbitMQEventSource) CommitEvents() {
	deliveryTag := atomic.LoadUint64(&rabbitEs.lastDeliveryTag)
	log.Infof("[RabbitMQEventSource] Going to commit messages prior to delivery tag %v", deliveryTag)
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
