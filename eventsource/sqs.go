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
	"time"
)

type SQSEventSource struct {
	awsSession *session.Session
	sqsService *sqs.SQS
	queueUrl   string
	workspace  string
	eventSink  chan *cloudevents.Event
	records    []*string
	consumed   map[string]*string
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
		records:    make([]*string, 0),
		consumed:   make(map[string]*string),
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
		seconds int64 = 1
	)

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
			if _, ok := sqsEs.consumed[*message.MessageId]; ok {
				continue
			}

			sqsEs.consumed[*message.MessageId] = message.ReceiptHandle
			rawBody := []byte(*message.Body)

			if fastjson.GetString(rawBody, "specversion") == "" {
				log.Debugf("[SQSEventSource] Received lambda destination event, cast to cloudevent")
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
			sqsEs.records = append(sqsEs.records, message.MessageId)
		}
	}
}

func (sqsEs *SQSEventSource) CommitEvents() {
	records := sqsEs.records

	recordsLen := len(records) / 10

	for i := 0; i < recordsLen; i++ {
		entries := make([]*sqs.DeleteMessageBatchRequestEntry, 10)
		for j := 0; j < 10; j++ {
			record := records[(10*i)+j]
			receiptHandle := sqsEs.consumed[*record]
			entries[j] = &sqs.DeleteMessageBatchRequestEntry{
				ReceiptHandle: receiptHandle,
				Id:            record,
			}
			delete(sqsEs.consumed, *record)
		}
		log.Debugf("[SQSEventSource] Going to commit %v events", len(entries))
		delMsgRequest := sqs.DeleteMessageBatchInput{
			Entries:  entries,
			QueueUrl: &sqsEs.queueUrl,
		}
		_, err := sqsEs.sqsService.DeleteMessageBatch(&delMsgRequest)
		if err != nil {
			panic(err)
		}
	}

	recordsMod := len(records) % 10
	if recordsMod != 0 {
		entries := make([]*sqs.DeleteMessageBatchRequestEntry, recordsMod)
		for j := 0; j < recordsMod; j++ {
			record := records[(10*recordsLen)+j]
			receiptHandle := sqsEs.consumed[*record]
			entries[j] = &sqs.DeleteMessageBatchRequestEntry{
				ReceiptHandle: receiptHandle,
				Id:            record,
			}
			delete(sqsEs.consumed, *record)
		}
		log.Debugf("[SQSEventSource] Going to commit %v events", len(entries))
		delMsgRequest := sqs.DeleteMessageBatchInput{
			Entries:  entries,
			QueueUrl: &sqsEs.queueUrl,
		}
		_, err := sqsEs.sqsService.DeleteMessageBatch(&delMsgRequest)
		if err != nil {
			panic(err)
		}
	}

	sqsEs.records = make([]*string, 0)
}

func (sqsEs *SQSEventSource) Stop() {
	panic("implement me")
}
