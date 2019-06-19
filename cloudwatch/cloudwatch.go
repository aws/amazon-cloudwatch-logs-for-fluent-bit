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
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
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

// LogsClient contains the CloudWatch API calls used by this plugin
type LogsClient interface {
	CreateLogGroup(input *cloudwatchlogs.CreateLogGroupInput) (*cloudwatchlogs.CreateLogGroupOutput, error)
	CreateLogStream(input *cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error)
	DescribeLogStreams(input *cloudwatchlogs.DescribeLogStreamsInput) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
	PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
}

type logStream struct {
	logEvents         []*cloudwatchlogs.InputLogEvent
	currentByteLength int
	nextSequenceToken *string
	logStreamName     string
}

// OutputPlugin is the CloudWatch Logs Fluent Bit output plugin
type OutputPlugin struct {
	logGroupName    string
	logStreamPrefix string
	logStreamName   string
	logKey          string
	client          LogsClient
	streams         map[string]*logStream
	backoff         *plugins.Backoff
	timer           *plugins.Timeout
}

// OutputPluginConfig is the input information used by NewOutputPlugin to create a new OutputPlugin
type OutputPluginConfig struct {
	Region          string
	LogGroupName    string
	LogStreamPrefix string
	LogStreamName   string
	LogKey          string
	RoleARN         string
	AutoCreateGroup bool
	CWEndpoint      string
}

// Validate checks the configuration input for an OutputPlugin instances
func (config OutputPluginConfig) Validate() error {
	errorStr := "%s is a required parameter"
	if config.Region == "" {
		return fmt.Errorf(errorStr, "region")
	}
	if config.LogGroupName == "" {
		return fmt.Errorf(errorStr, "log_group_name")
	}
	if config.LogStreamName == "" && config.LogStreamPrefix == "" {
		return fmt.Errorf("log_stream_name or log_stream_prefix is required")
	}

	return nil
}

// NewOutputPlugin creates a OutputPlugin object
func NewOutputPlugin(config OutputPluginConfig) (*OutputPlugin, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(config.Region),
	})
	if err != nil {
		return nil, err
	}

	client := newCloudWatchLogsClient(config.RoleARN, sess, config.CWEndpoint)

	timer, err := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[cloudwatch] timeout threshold reached: Failed to send logs for %s\n", d.String())
		logrus.Error("[cloudwatch] Quitting Fluent Bit")
		os.Exit(1)
	})

	if err != nil {
		return nil, err
	}

	if config.AutoCreateGroup {
		err = createLogGroup(config.LogGroupName, client)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
					return nil, err
				}
				logrus.Infof("[cloudwatch] Log group %s already exists\n", config.LogGroupName)
			} else {
				return nil, err
			}
		}
		logrus.Infof("Created log group %s\n", config.LogGroupName)
	}

	return &OutputPlugin{
		logGroupName:    config.LogGroupName,
		logStreamPrefix: config.LogStreamPrefix,
		logStreamName:   config.LogStreamName,
		logKey:          config.LogKey,
		client:          client,
		timer:           timer,
		streams:         make(map[string]*logStream),
	}, nil
}

func newCloudWatchLogsClient(roleARN string, sess *session.Session, endpoint string) *cloudwatchlogs.CloudWatchLogs {
	svcConfig := aws.Config{}
	if endpoint != "" {
		defaultResolver := endpoints.DefaultResolver()
		cwCustomResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == "logs" {
				return endpoints.ResolvedEndpoint{
					URL: endpoint,
				}, nil
			}
			return defaultResolver.EndpointFor(service, region, optFns...)
		}
		svcConfig.EndpointResolver = endpoints.ResolverFunc(cwCustomResolverFn)
	}
	if roleARN != "" {
		creds := stscreds.NewCredentials(sess, roleARN)
		return cloudwatchlogs.New(sess, &aws.Config{Credentials: creds})
	}

	return cloudwatchlogs.New(sess)
}

// AddEvent accepts a record and adds it to the buffer for its stream, flushing the buffer if it is full
// the return value is one of: FLB_OK, FLB_RETRY
// API Errors lead to an FLB_RETRY, and all other errors are logged, the record is discarded and FLB_OK is returned
func (output *OutputPlugin) AddEvent(tag string, record map[interface{}]interface{}, timestamp time.Time) int {
	data, err := output.processRecord(record)
	if err != nil {
		logrus.Errorf("[cloudwatch] %v\n", err)
		// discard this single bad record and let the batch continue
		return fluentbit.FLB_OK
	}

	event := logString(data)

	stream, err := output.getLogStream(tag)
	if err != nil {
		logrus.Errorf("[cloudwatch] %v\n", err)
		// an error means that the log stream was not created; this is retryable
		return fluentbit.FLB_RETRY
	}

	if len(stream.logEvents) == maximumLogEventsPerPut || (stream.currentByteLength+cloudwatchLen(event)) >= maximumBytesPerPut {
		err = output.putLogEvents(stream)
		if err != nil {
			logrus.Errorf("[cloudwatch] %v\n", err)
			// send failures are retryable
			return fluentbit.FLB_RETRY
		}
	}

	stream.logEvents = append(stream.logEvents, &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(event),
		Timestamp: aws.Int64(timestamp.UnixNano() / 1e6), // CloudWatch uses milliseconds since epoch
	})
	stream.currentByteLength += cloudwatchLen(event)
	return fluentbit.FLB_OK
}

func (output *OutputPlugin) getLogStream(tag string) (*logStream, error) {
	// find log stream by tag
	stream, ok := output.streams[tag]
	if !ok {
		// stream doesn't exist, create it
		stream, err := output.createStream(tag)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
					// existing stream
					return output.existingLogStream(tag)
				}
			}
		}

		return stream, err
	}

	return stream, nil
}

func (output *OutputPlugin) existingLogStream(tag string) (*logStream, error) {
	var nextToken *string
	var stream *logStream
	name := output.getStreamName(tag)

	for stream == nil {
		resp, err := output.describeLogStreams(name, nextToken)
		if err != nil {
			return nil, err
		}

		for _, result := range resp.LogStreams {
			if aws.StringValue(result.LogStreamName) == name {
				stream = &logStream{
					logStreamName:     name,
					logEvents:         make([]*cloudwatchlogs.InputLogEvent, 0, maximumLogEventsPerPut),
					nextSequenceToken: result.UploadSequenceToken,
				}

				output.streams[tag] = stream
			}
		}

		if resp.NextToken == nil {
			return nil, fmt.Errorf("error: does not compute: Log Stream %s could not be created, but also could not be found in the log group", name)
		}

		nextToken = resp.NextToken
	}
	return stream, nil
}

func (output *OutputPlugin) describeLogStreams(name string, nextToken *string) (*cloudwatchlogs.DescribeLogStreamsOutput, error) {
	output.timer.Check()
	resp, err := output.client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(output.logGroupName),
		LogStreamNamePrefix: aws.String(name),
		NextToken:           nextToken,
	})

	if err != nil {
		output.timer.Start()
		return nil, err
	}
	output.timer.Reset()

	return resp, err
}

func (output *OutputPlugin) getStreamName(tag string) string {
	name := output.logStreamName
	if output.logStreamPrefix != "" {
		name = output.logStreamPrefix + tag
	}

	return name
}

func (output *OutputPlugin) createStream(tag string) (*logStream, error) {
	output.timer.Check()
	name := output.getStreamName(tag)
	_, err := output.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(output.logGroupName),
		LogStreamName: aws.String(name),
	})

	if err != nil {
		output.timer.Start()
		return nil, err
	}
	output.timer.Reset()

	stream := &logStream{
		logStreamName:     name,
		logEvents:         make([]*cloudwatchlogs.InputLogEvent, 0, maximumLogEventsPerPut),
		nextSequenceToken: nil, // sequence token not required for a new log stream
	}

	output.streams[tag] = stream
	logrus.Debugf("[cloudwatch] Created log stream %s", name)

	return stream, nil
}

func createLogGroup(name string, client LogsClient) error {
	_, err := client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(name),
	})

	return err
}

// Takes the byte slice and returns a string
// Also removes leading and trailing whitespace
func logString(record []byte) string {
	return strings.TrimSpace(string(record))
}

func (output *OutputPlugin) processRecord(record map[interface{}]interface{}) ([]byte, error) {
	var err error
	record, err = plugins.DecodeMap(record)
	if err != nil {
		logrus.Debugf("[cloudwatch] Failed to decode record: %v\n", record)
		return nil, err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	var data []byte

	if output.logKey != "" {
		log, err := logKey(record, output.logKey)
		if err != nil {
			return nil, err
		}

		data, err = json.Marshal(log)
	} else {
		data, err = json.Marshal(record)
	}

	if err != nil {
		logrus.Debugf("[cloudwatch] Failed to marshal record: %v\nLog Key: %s\n", record, output.logKey)
		return nil, err
	}

	return data, nil
}

// Implements the log_key option, which allows customers to only send the value of a given key to CW Logs
func logKey(record map[interface{}]interface{}, logKey string) (*interface{}, error) {
	for key, val := range record {
		var currentKey string
		switch t := key.(type) {
		case []byte:
			currentKey = string(t)
		case string:
			currentKey = t
		default:
			logrus.Debugf("[go plugin]: Unable to determine type of key %v\n", t)
			continue
		}

		if logKey == currentKey {
			return &val, nil
		}

	}

	return nil, fmt.Errorf("Failed to find key %s specified by log_key option in log record: %v", logKey, record)
}

// Flush sends the current buffer of records (for the stream that corresponds with the given tag)
func (output *OutputPlugin) Flush(tag string) error {
	stream, err := output.getLogStream(tag)
	if err != nil {
		return err
	}
	return output.putLogEvents(stream)
}

func (output *OutputPlugin) putLogEvents(stream *logStream) error {
	output.timer.Check()

	// Log events in a single PutLogEvents request must be in chronological order.
	sort.Slice(stream.logEvents, func(i, j int) bool {
		return aws.Int64Value(stream.logEvents[i].Timestamp) < aws.Int64Value(stream.logEvents[j].Timestamp)
	})
	response, err := output.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     stream.logEvents,
		LogGroupName:  aws.String(output.logGroupName),
		LogStreamName: aws.String(stream.logStreamName),
		SequenceToken: stream.nextSequenceToken,
	})
	if err != nil {
		output.timer.Start()
		return err
	}
	output.timer.Reset()
	logrus.Debugf("Sent %d events to CloudWatch\n", len(stream.logEvents))

	stream.nextSequenceToken = response.NextSequenceToken
	stream.logEvents = stream.logEvents[:0]
	stream.currentByteLength = 0

	return nil
}

func processRejectedEventsInfo(response *cloudwatchlogs.PutLogEventsOutput) {
	if response.RejectedLogEventsInfo != nil {
		if response.RejectedLogEventsInfo.ExpiredLogEventEndIndex != nil {
			logrus.Warnf("[cloudwatch] %d log events were marked as expired by CloudWatch\n", aws.Int64Value(response.RejectedLogEventsInfo.ExpiredLogEventEndIndex))
		}
		if response.RejectedLogEventsInfo.TooNewLogEventStartIndex != nil {
			logrus.Warnf("[cloudwatch] %d log events were marked as too new by CloudWatch\n", aws.Int64Value(response.RejectedLogEventsInfo.TooNewLogEventStartIndex))
		}
		if response.RejectedLogEventsInfo.TooOldLogEventEndIndex != nil {
			logrus.Warnf("[cloudwatch] %d log events were marked as too old by CloudWatch\n", aws.Int64Value(response.RejectedLogEventsInfo.TooOldLogEventEndIndex))
		}
	}
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
