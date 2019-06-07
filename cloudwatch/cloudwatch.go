// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//	http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package cloudwatch

import (
	"fmt"
	"os"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/awslabs/amazon-cloudwatch-logs-for-fluent-bit/plugins"
	fluentbit "github.com/fluent/fluent-bit-go/output"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

const (
	// See: http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	perEventBytes          = 26
	maximumBytesPerPut     = 1048576
	maximumLogEventsPerPut = 10000
)

type CloudWatchLogsClient interface {
	CreateLogGroup(input *cloudwatchlogs.CreateLogGroupInput) (*cloudwatchlogs.CreateLogGroupOutput, error)
	CreateLogStream(input *cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error)
	PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
}

type logStream struct {
	logEvents         []*cloudwatchlogs.InputLogEvent
	currentByteLength int
	nextSequenceToken *string
	logStreamName     string
}

type OutputPlugin struct {
	region          string
	logGroupName    string
	logStreamPrefix string
	client          CloudWatchLogsClient
	streams         map[string]*logStream
	backoff         *plugins.Backoff
	timer           *plugins.Timeout
}

// NewOutputPlugin creates a OutputPlugin object
func NewOutputPlugin(region string, logGroupName string, logStreamPrefix string, roleARN string, autoCreateGroup bool) (*OutputPlugin, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	client := newCloudWatchLogsClient(roleARN, sess)

	timer, err := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[cloudwatch] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[cloudwatch] Quitting Fluent Bit")
		os.Exit(1)
	})

	if err != nil {
		return nil, err
	}

	if autoCreateGroup {
		err = createLogGroup(logGroupName, client)
		if err != nil {
			return nil, err
		}
	}

	return &OutputPlugin{
		region:          region,
		logGroupName:    logGroupName,
		logStreamPrefix: logStreamPrefix,
		client:          client,
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}, nil
}

func newCloudWatchLogsClient(roleARN string, sess *session.Session) *cloudwatchlogs.CloudWatchLogs {
	if roleARN != "" {
		creds := stscreds.NewCredentials(sess, roleARN)
		return cloudwatchlogs.New(sess, &aws.Config{Credentials: creds})
	}

	return cloudwatchlogs.New(sess)
}

func (output *OutputPlugin) AddEvent(tag string, record map[interface{}]interface{}, timestamp time.Time) (int, error) {
	data, retCode, err := output.processRecord(record)
	if err != nil {
		return retCode, err
	}
	event := string(data)

	stream, err := output.getLogStream(tag)
	if err != nil {
		return fluentbit.FLB_ERROR, err
	}

	if len(stream.logEvents) == maximumLogEventsPerPut || (stream.currentByteLength+cloudwatchLen(event)) >= maximumBytesPerPut {
		err = output.putLogEvents(stream)
		if err != nil {
			return fluentbit.FLB_ERROR, err
		}
	}

	stream.logEvents = append(stream.logEvents, &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(event),
		Timestamp: aws.Int64(timestamp.Unix()),
	})
	stream.currentByteLength += cloudwatchLen(event)
	return fluentbit.FLB_ERROR, nil
}

func (output *OutputPlugin) getLogStream(tag string) (*logStream, error) {
	// find log stream by tag
	stream, ok := output.streams[tag]
	if !ok {
		// stream doesn't exist, create it
		return output.createStream(output.logStreamPrefix+tag, tag)
	}

	return stream, nil
}

func (output *OutputPlugin) createStream(name, tag string) (*logStream, error) {
	_, err := output.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(output.logGroupName),
		LogStreamName: aws.String(name),
	})

	if err != nil {
		return nil, err
	}

	stream := &logStream{
		logStreamName:     name,
		logEvents:         make([]*cloudwatchlogs.InputLogEvent, 0, maximumLogEventsPerPut),
		nextSequenceToken: nil, // sequence token not required for a new log stream
	}

	output.streams[tag] = stream

	return stream, nil
}

func createLogGroup(name string, client CloudWatchLogsClient) error {
	_, err := client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(name),
	})

	return err
}

func (output *OutputPlugin) processRecord(record map[interface{}]interface{}) ([]byte, int, error) {
	var err error
	record, err = plugins.DecodeMap(record)
	if err != nil {
		logrus.Debugf("[cloudwatch] Failed to decode record: %v\n", record)
		return nil, fluentbit.FLB_ERROR, err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(record)
	if err != nil {
		logrus.Debugf("[cloudwatch] Failed to marshal record: %v\n", record)
		return nil, fluentbit.FLB_ERROR, err
	}

	return data, fluentbit.FLB_OK, nil
}

func (output *OutputPlugin) Flush(tag string) error {
	stream, err := output.getLogStream(tag)
	if err != nil {
		return err
	}
	return output.putLogEvents(stream)
}

func (output *OutputPlugin) putLogEvents(stream *logStream) error {
	fmt.Println("Sending to CloudWatch")
	response, err := output.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     stream.logEvents,
		LogGroupName:  aws.String(output.logGroupName),
		LogStreamName: aws.String(stream.logStreamName),
		SequenceToken: stream.nextSequenceToken,
	})
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("Sent %d events to CloudWatch\n", len(stream.logEvents))

	stream.nextSequenceToken = response.NextSequenceToken
	stream.logEvents = stream.logEvents[:0]
	stream.currentByteLength = 0

	return nil
}

// effectiveLen counts the effective number of bytes in the string, after
// UTF-8 normalization.  UTF-8 normalization includes replacing bytes that do
// not constitute valid UTF-8 encoded Unicode codepoints with the Unicode
// replacement codepoint U+FFFD (a 3-byte UTF-8 sequence, represented in Go as
// utf8.RuneError)
func effectiveLen(line string) int {
	effectiveBytes := 0
	for _, rune := range line {
		effectiveBytes += utf8.RuneLen(rune)
	}
	return effectiveBytes
}

func cloudwatchLen(event string) int {
	return effectiveLen(event) + perEventBytes
}
