package eventsource

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	cloudevents "github.com/cloudevents/sdk-go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"sync"
	"time"
)

type SQSEventSource struct {
	awsSession *session.Session
	sqsService *sqs.SQS
	queueUrl   string
	workspace  string
	eventSink  chan *cloudevents.Event
	consumed   sync.Map
	records    sync.Map
}

func CreateSQSEventSource(workspace string, eventSink chan *cloudevents.Event, accessKeyID string, secretAccessKey string, region string, queue string) EventSource {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
		Region:      aws.String(region),
	})
	if err != nil {
		panic(err)
	}

	sqsService := sqs.New(sess)

	queueUrlOutput, err := sqsService.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		panic(err)
	}

	sqsEs := &SQSEventSource{
		awsSession: sess,
		sqsService: sqsService,
		queueUrl:   *queueUrlOutput.QueueUrl,
		workspace:  workspace,
		eventSink:  eventSink,
		records:    sync.Map{},
		consumed:   sync.Map{},
	}

	return sqsEs
}

func CreateSQSEventSourceMappedConfig(workspace string, eventSink chan *cloudevents.Event, config json.RawMessage) EventSource {
	accessKeyID := fastjson.GetString(config, "access_key_id")
	secretAccessKey := fastjson.GetString(config, "secret_access_key")
	region := fastjson.GetString(config, "region")
	queue := fastjson.GetString(config, "queue")
	return CreateSQSEventSource(workspace, eventSink, accessKeyID, secretAccessKey, region, queue)
}

func (sqsEs *SQSEventSource) StartConsuming() {
	var (
		obj []byte
		p   fastjson.Parser
	)
	seconds := int64(1)
	first := true
	log.Infof("[SQSEventSource] Starting to consume from queue %s", sqsEs.queueUrl)
	for {
		receiveMessageOutput, err := sqsEs.sqsService.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:        &sqsEs.queueUrl,
			WaitTimeSeconds: &seconds,
		})
		if err != nil {
			panic(err)
		}

		if first {
			fmt.Println(time.Now().UTC().UnixNano())
			first = false
		}

		for _, message := range receiveMessageOutput.Messages {
			if _, ok := sqsEs.consumed.Load(*message.MessageId); ok {
				continue
			}

			message := message
			go func() {
				var record []string

				sqsEs.consumed.Store(*message.MessageId, message.ReceiptHandle)
				rawBody := []byte(*message.Body)

				if fastjson.GetString(rawBody, "specversion") == "" {
					log.Infof("[SQSEventSource] Received lambda destination event, cast to cloudevent")
					parsed, err := p.ParseBytes(rawBody)
					if err != nil {
						panic(err)
					}
					obj = parsed.GetObject("responsePayload").MarshalTo(make([]byte, 0))
				} else {
					obj = rawBody
				}

				cloudevent, err := DecodeCloudEventBytes(obj)
				if err != nil {
					panic(err)
				}

				sqsEs.eventSink <- cloudevent

				value, ok := sqsEs.records.Load(cloudevent.Subject())
				if !ok {
					record = make([]string, 0)
				} else {
					record = value.([]string)
				}
				record = append(record, *message.MessageId)
				sqsEs.records.Store(cloudevent.Subject(), record)
			}()

		}
	}
}

func (sqsEs *SQSEventSource) CommitEvents(subject string) {
	var receiptHandle *string
	value, ok := sqsEs.records.Load(subject)
	if !ok {
		return
	}
	records := value.([]string)
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(records))

	for i, record := range records {
		value, ok := sqsEs.consumed.Load(record)
		if ok {
			receiptHandle = value.(*string)
		}
		entries[i] = &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: receiptHandle,
			Id: aws.String(record),
		}
		sqsEs.consumed.Delete(record)
	}

	delMsgRequest := sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: &sqsEs.queueUrl,
	}

	log.Debugf("[SQSEventSource] Going to commit %d events (subject %s)", len(entries), subject)
	_, err := sqsEs.sqsService.DeleteMessageBatch(&delMsgRequest)
	if err != nil {
		panic(err)
	}

	sqsEs.records.Delete(subject)
}

func (sqsEs *SQSEventSource) Stop() {
	panic("implement me")
}
