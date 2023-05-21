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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	fluentbit "github.com/fluent/fluent-bit-go/output"
	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/ksuid"
	"github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fasttemplate"
)

const (
	// See: http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	perEventBytes          = 26
	maximumBytesPerPut     = 1048576
	maximumLogEventsPerPut = 10000
	maximumBytesPerEvent   = 1024 * 256 //256KB
	maximumTimeSpanPerPut  = time.Hour * 24
	truncatedSuffix        = "[Truncated...]"
	maxGroupStreamLength   = 512
)

const (
	// Log stream objects that are empty and inactive for longer than the timeout get cleaned up
	logStreamInactivityTimeout = time.Hour
	// Check for expired log streams every 10 minutes
	logStreamInactivityCheckInterval = 10 * time.Minute
	// linuxBaseUserAgent is the base user agent string used for Linux.
	linuxBaseUserAgent = "aws-fluent-bit-plugin"
	// windowsBaseUserAgent is the base user agent string used for Windows.
	windowsBaseUserAgent = "aws-fluent-bit-plugin-windows"
)

// LogsClient contains the CloudWatch API calls used by this plugin
type LogsClient interface {
	CreateLogGroup(input *cloudwatchlogs.CreateLogGroupInput) (*cloudwatchlogs.CreateLogGroupOutput, error)
	PutRetentionPolicy(input *cloudwatchlogs.PutRetentionPolicyInput) (*cloudwatchlogs.PutRetentionPolicyOutput, error)
	CreateLogStream(input *cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error)
	DescribeLogStreams(input *cloudwatchlogs.DescribeLogStreamsInput) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
	PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
}

type logStream struct {
	logEvents         []*cloudwatchlogs.InputLogEvent
	currentByteLength int
	currentBatchStart *time.Time
	currentBatchEnd   *time.Time
	nextSequenceToken *string
	logStreamName     string
	logGroupName      string
	expiration        time.Time
}

// Event is the input data and contains a log entry.
// The group and stream are added during processing.
type Event struct {
	TS     time.Time
	Record map[interface{}]interface{}
	Tag    string
	group  string
	stream string
}

// TaskMetadata it the task metadata from ECS V3 endpoint
type TaskMetadata struct {
	Cluster string `json:"Cluster,omitempty"`
	TaskARN string `json:"TaskARN,omitempty"`
	TaskID  string `json:"TaskID,omitempty"`
}

type streamDoesntExistError struct {
	streamName string
	groupName  string
}

func (stream *logStream) isExpired() bool {
	if len(stream.logEvents) == 0 && stream.expiration.Before(time.Now()) {
		return true
	}
	return false
}

func (stream *logStream) updateExpiration() {
	stream.expiration = time.Now().Add(logStreamInactivityTimeout)
}

type fastTemplate struct {
	String string
	*fasttemplate.Template
}

// OutputPlugin is the CloudWatch Logs Fluent Bit output plugin
type OutputPlugin struct {
	logGroupName                  *fastTemplate
	defaultLogGroupName           string
	logStreamPrefix               string
	logStreamName                 *fastTemplate
	defaultLogStreamName          string
	logKey                        string
	client                        LogsClient
	streams                       map[string]*logStream
	groups                        map[string]struct{}
	timer                         *plugins.Timeout
	nextLogStreamCleanUpCheckTime time.Time
	PluginInstanceID              int
	logGroupTags                  map[string]*string
	logGroupRetention             int64
	autoCreateGroup               bool
	autoCreateStream              bool
	bufferPool                    bytebufferpool.Pool
	ecsMetadata                   TaskMetadata
	runningInECS                  bool
	uuid                          string
	extraUserAgent                string
}

// OutputPluginConfig is the input information used by NewOutputPlugin to create a new OutputPlugin
type OutputPluginConfig struct {
	Region               string
	LogGroupName         string
	DefaultLogGroupName  string
	LogStreamPrefix      string
	LogStreamName        string
	DefaultLogStreamName string
	LogKey               string
	RoleARN              string
	AutoCreateGroup      bool
	AutoCreateStream     bool
	NewLogGroupTags      string
	LogRetentionDays     int64
	CWEndpoint           string
	STSEndpoint          string
	ExternalID           string
	CredsEndpoint        string
	PluginInstanceID     int
	LogFormat            string
	ExtraUserAgent       string
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

	if config.LogStreamName != "" && config.LogStreamPrefix != "" {
		return fmt.Errorf("either log_stream_name or log_stream_prefix can be configured. They cannot be provided together")
	}

	return nil
}

// NewOutputPlugin creates a OutputPlugin object
func NewOutputPlugin(config OutputPluginConfig) (*OutputPlugin, error) {
	logrus.Debugf("[cloudwatch %d] Initializing NewOutputPlugin", config.PluginInstanceID)

	client, err := newCloudWatchLogsClient(config)
	if err != nil {
		return nil, err
	}

	timer, err := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[cloudwatch %d] timeout threshold reached: Failed to send logs for %s\n", config.PluginInstanceID, d.String())
		logrus.Fatalf("[cloudwatch %d] Quitting Fluent Bit", config.PluginInstanceID) // exit the plugin and kill Fluent Bit
	})
	if err != nil {
		return nil, err
	}

	logGroupTemplate, err := newTemplate(config.LogGroupName)
	if err != nil {
		return nil, err
	}

	logStreamTemplate, err := newTemplate(config.LogStreamName)
	if err != nil {
		return nil, err
	}

	runningInECS := true
	// check if it is running in ECS
	if os.Getenv("ECS_CONTAINER_METADATA_URI") == "" {
		runningInECS = false
	}

	return &OutputPlugin{
		logGroupName:                  logGroupTemplate,
		logStreamName:                 logStreamTemplate,
		logStreamPrefix:               config.LogStreamPrefix,
		defaultLogGroupName:           config.DefaultLogGroupName,
		defaultLogStreamName:          config.DefaultLogStreamName,
		logKey:                        config.LogKey,
		client:                        client,
		timer:                         timer,
		streams:                       make(map[string]*logStream),
		nextLogStreamCleanUpCheckTime: time.Now().Add(logStreamInactivityCheckInterval),
		PluginInstanceID:              config.PluginInstanceID,
		logGroupTags:                  tagKeysToMap(config.NewLogGroupTags),
		logGroupRetention:             config.LogRetentionDays,
		autoCreateGroup:               config.AutoCreateGroup,
		autoCreateStream:              config.AutoCreateStream,
		groups:                        make(map[string]struct{}),
		ecsMetadata:                   TaskMetadata{},
		runningInECS:                  runningInECS,
		uuid:                          ksuid.New().String(),
		extraUserAgent:                config.ExtraUserAgent,
	}, nil
}

func newCloudWatchLogsClient(config OutputPluginConfig) (*cloudwatchlogs.CloudWatchLogs, error) {
	customResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == endpoints.LogsServiceID && config.CWEndpoint != "" {
			return endpoints.ResolvedEndpoint{
				URL: config.CWEndpoint,
			}, nil
		} else if service == endpoints.StsServiceID && config.STSEndpoint != "" {
			return endpoints.ResolvedEndpoint{
				URL: config.STSEndpoint,
			}, nil
		}
		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
	}

	// Fetch base credentials
	baseConfig := &aws.Config{
		Region:                        aws.String(config.Region),
		EndpointResolver:              endpoints.ResolverFunc(customResolverFn),
		CredentialsChainVerboseErrors: aws.Bool(true),
	}

	if config.CredsEndpoint != "" {
		creds := endpointcreds.NewCredentialsClient(*baseConfig, request.Handlers{}, config.CredsEndpoint,
			func(provider *endpointcreds.Provider) {
				provider.ExpiryWindow = 5 * time.Minute
			})
		baseConfig.Credentials = creds
	}

	sess, err := session.NewSession(baseConfig)
	if err != nil {
		return nil, err
	}

	var svcSess = sess
	var svcConfig = baseConfig
	eksRole := os.Getenv("EKS_POD_EXECUTION_ROLE")
	if eksRole != "" {
		logrus.Debugf("[cloudwatch %d] Fetching EKS pod credentials.\n", config.PluginInstanceID)
		eksConfig := &aws.Config{}
		creds := stscreds.NewCredentials(svcSess, eksRole)
		eksConfig.Credentials = creds
		eksConfig.Region = aws.String(config.Region)
		svcConfig = eksConfig

		svcSess, err = session.NewSession(svcConfig)
		if err != nil {
			return nil, err
		}
	}

	if config.RoleARN != "" {
		logrus.Debugf("[cloudwatch %d] Fetching credentials for %s\n", config.PluginInstanceID, config.RoleARN)
		stsConfig := &aws.Config{}
		creds := stscreds.NewCredentials(svcSess, config.RoleARN, func(p *stscreds.AssumeRoleProvider) {
			if config.ExternalID != "" {
				p.ExternalID = aws.String(config.ExternalID)
			}
		})
		stsConfig.Credentials = creds
		stsConfig.Region = aws.String(config.Region)
		svcConfig = stsConfig

		svcSess, err = session.NewSession(svcConfig)
		if err != nil {
			return nil, err
		}
	}

	client := cloudwatchlogs.New(svcSess, svcConfig)
	client.Handlers.Build.PushBackNamed(customUserAgentHandler(config))
	if config.LogFormat != "" {
		client.Handlers.Build.PushBackNamed(LogFormatHandler(config.LogFormat))
	}
	return client, nil
}

// CustomUserAgentHandler returns a http request handler that sets a custom user agent to all aws requests
func customUserAgentHandler(config OutputPluginConfig) request.NamedHandler {
	const userAgentHeader = "User-Agent"

	baseUserAgent := linuxBaseUserAgent
	if runtime.GOOS == "windows" {
		baseUserAgent = windowsBaseUserAgent
	}

	return request.NamedHandler{
		Name: "ECSLocalEndpointsAgentHandler",
		Fn: func(r *request.Request) {
			currentAgent := r.HTTPRequest.Header.Get(userAgentHeader)
			if config.ExtraUserAgent != "" {
				r.HTTPRequest.Header.Set(userAgentHeader,
					fmt.Sprintf("%s-%s (%s) %s", baseUserAgent, config.ExtraUserAgent, runtime.GOOS, currentAgent))
			} else {
				r.HTTPRequest.Header.Set(userAgentHeader,
					fmt.Sprintf("%s (%s) %s", baseUserAgent, runtime.GOOS, currentAgent))
			}
		},
	}
}

// AddEvent accepts a record and adds it to the buffer for its stream, flushing the buffer if it is full
// the return value is one of: FLB_OK, FLB_RETRY
// API Errors lead to an FLB_RETRY, and all other errors are logged, the record is discarded and FLB_OK is returned
func (output *OutputPlugin) AddEvent(e *Event) int {
	// Step 1: convert the Event data to strings, and check for a log key.
	data, err := output.processRecord(e)
	if err != nil {
		logrus.Errorf("[cloudwatch %d] %v\n", output.PluginInstanceID, err)
		// discard this single bad record and let the batch continue
		return fluentbit.FLB_OK
	}

	// Step 2. Make sure the Event data isn't empty.
	eventString := logString(data)
	if len(eventString) == 0 {
		logrus.Debugf("[cloudwatch %d] Discarding an event from publishing as it is empty\n", output.PluginInstanceID)
		// discard this single empty record and let the batch continue
		return fluentbit.FLB_OK
	}

	// Step 3. Extract the Task Metadata if applicable.
	if output.runningInECS && output.ecsMetadata.TaskID == "" {
		err := output.getECSMetadata()
		if err != nil {
			logrus.Errorf("[cloudwatch %d] Failed to get ECS Task Metadata with error: %v\n", output.PluginInstanceID, err)
			return fluentbit.FLB_RETRY
		}
	}

	// Step 4. Assign a log group and log stream name to the Event.
	output.setGroupStreamNames(e)

	// Step 5. Create a missing log group for this Event.
	if _, ok := output.groups[e.group]; !ok {
		logrus.Debugf("[cloudwatch %d] Finding log group: %s", output.PluginInstanceID, e.group)

		if err := output.createLogGroup(e); err != nil {
			logrus.Error(err)
			return fluentbit.FLB_RETRY
		}

		output.groups[e.group] = struct{}{}
	}

	// Step 6. Create or retrieve an existing log stream for this Event.
	stream, err := output.getLogStream(e)
	if err != nil {
		logrus.Errorf("[cloudwatch %d] %v\n", output.PluginInstanceID, err)
		// an error means that the log stream was not created; this is retryable
		return fluentbit.FLB_RETRY
	}

	// Step 7. Check batch limits and flush buffer if any of these limits will be exeeded by this log Entry.
	countLimit := len(stream.logEvents) == maximumLogEventsPerPut
	sizeLimit := (stream.currentByteLength + cloudwatchLen(eventString)) >= maximumBytesPerPut
	spanLimit := stream.logBatchSpan(e.TS) >= maximumTimeSpanPerPut
	if countLimit || sizeLimit || spanLimit {
		err = output.putLogEvents(stream)
		if err != nil {
			logrus.Errorf("[cloudwatch %d] %v\n", output.PluginInstanceID, err)
			// send failures are retryable
			return fluentbit.FLB_RETRY
		}
	}

	// Step 8. Add this event to the running tally.
	stream.logEvents = append(stream.logEvents, &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(eventString),
		Timestamp: aws.Int64(e.TS.UnixNano() / 1e6), // CloudWatch uses milliseconds since epoch
	})
	stream.currentByteLength += cloudwatchLen(eventString)
	if stream.currentBatchStart == nil || stream.currentBatchStart.After(e.TS) {
		stream.currentBatchStart = &e.TS
	}
	if stream.currentBatchEnd == nil || stream.currentBatchEnd.Before(e.TS) {
		stream.currentBatchEnd = &e.TS
	}

	return fluentbit.FLB_OK
}

// This plugin tracks CW Log streams
// We need to periodically delete any streams that haven't been written to in a while
// Because each stream incurs some memory for its buffer of log events
// (Which would be empty for an unused stream)
func (output *OutputPlugin) cleanUpExpiredLogStreams() {
	if output.nextLogStreamCleanUpCheckTime.Before(time.Now()) {
		logrus.Debugf("[cloudwatch %d] Checking for expired log streams", output.PluginInstanceID)

		for name, stream := range output.streams {
			if stream.isExpired() {
				logrus.Debugf("[cloudwatch %d] Removing internal buffer for log stream %s in group %s; the stream has not been written to for %s",
					output.PluginInstanceID, stream.logStreamName, stream.logGroupName, logStreamInactivityTimeout.String())
				delete(output.streams, name)
			}
		}
		output.nextLogStreamCleanUpCheckTime = time.Now().Add(logStreamInactivityCheckInterval)
	}
}

func (err *streamDoesntExistError) Error() string {
	return fmt.Sprintf("error: stream %s doesn't exist in log group %s", err.streamName, err.groupName)
}

func (output *OutputPlugin) getLogStream(e *Event) (*logStream, error) {
	stream, ok := output.streams[e.group+e.stream]
	if !ok {
		// assume the stream exists
		stream, err := output.existingLogStream(e)
		if err != nil {
			// if it doesn't then create it
			if _, ok := err.(*streamDoesntExistError); ok {
				return output.createStream(e)
			}
		}
		return stream, err
	}
	return stream, nil
}

func (output *OutputPlugin) existingLogStream(e *Event) (*logStream, error) {
	var nextToken *string
	var stream *logStream

	for stream == nil {
		resp, err := output.describeLogStreams(e, nextToken)
		if err != nil {
			return nil, err
		}

		for _, result := range resp.LogStreams {
			if aws.StringValue(result.LogStreamName) == e.stream {
				stream = &logStream{
					logGroupName:      e.group,
					logStreamName:     e.stream,
					logEvents:         make([]*cloudwatchlogs.InputLogEvent, 0, maximumLogEventsPerPut),
					nextSequenceToken: result.UploadSequenceToken,
				}
				output.streams[e.group+e.stream] = stream

				logrus.Debugf("[cloudwatch %d] Initializing internal buffer for exising log stream %s\n", output.PluginInstanceID, e.stream)
				stream.updateExpiration() // initialize

				break
			}
		}

		if stream == nil && resp.NextToken == nil {
			logrus.Infof("[cloudwatch %d] Log stream %s does not exist in log group %s", output.PluginInstanceID, e.stream, e.group)
			return nil, &streamDoesntExistError{
				streamName: e.stream,
				groupName:  e.group,
			}
		}

		nextToken = resp.NextToken
	}
	return stream, nil
}

func (output *OutputPlugin) describeLogStreams(e *Event, nextToken *string) (*cloudwatchlogs.DescribeLogStreamsOutput, error) {
	output.timer.Check()
	resp, err := output.client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(e.group),
		LogStreamNamePrefix: aws.String(e.stream),
		NextToken:           nextToken,
	})

	if err != nil {
		output.timer.Start()
		return nil, err
	}
	output.timer.Reset()

	return resp, err
}

// setGroupStreamNames adds the log group and log stream names to the event struct.
// This happens by parsing (any) template data in either configured name.
func (output *OutputPlugin) setGroupStreamNames(e *Event) {
	// This happens here to avoid running Split more than once per log Event.
	logTagSplit := strings.SplitN(e.Tag, ".", 10)
	s := &sanitizer{sanitize: sanitizeGroup, buf: output.bufferPool.Get()}

	if _, err := parseDataMapTags(e, logTagSplit, output.logGroupName, output.ecsMetadata, output.uuid, s); err != nil {
		e.group = output.defaultLogGroupName
		logrus.Errorf("[cloudwatch %d] parsing log_group_name template '%s' "+
			"(using value of default_log_group_name instead): %v",
			output.PluginInstanceID, output.logGroupName.String, err)
	} else if e.group = s.buf.String(); len(e.group) == 0 {
		e.group = output.defaultLogGroupName
	} else if len(e.group) > maxGroupStreamLength {
		e.group = e.group[:maxGroupStreamLength]
	}

	if output.logStreamPrefix != "" {
		e.stream = output.logStreamPrefix + e.Tag
		output.bufferPool.Put(s.buf)

		return
	}

	s.sanitize = sanitizeStream
	s.buf.Reset()

	if _, err := parseDataMapTags(e, logTagSplit, output.logStreamName, output.ecsMetadata, output.uuid, s); err != nil {
		e.stream = output.defaultLogStreamName
		logrus.Errorf("[cloudwatch %d] parsing log_stream_name template '%s': %v",
			output.PluginInstanceID, output.logStreamName.String, err)
	} else if e.stream = s.buf.String(); len(e.stream) == 0 {
		e.stream = output.defaultLogStreamName
	} else if len(e.stream) > maxGroupStreamLength {
		e.stream = e.stream[:maxGroupStreamLength]
	}

	output.bufferPool.Put(s.buf)
}

func (output *OutputPlugin) createStream(e *Event) (*logStream, error) {
	if !output.autoCreateStream {
		return nil, fmt.Errorf("error: attempting to create log Stream %s in log group %s however auto_create_stream is disabled", e.stream, e.group)
	}
	output.timer.Check()
	_, err := output.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(e.group),
		LogStreamName: aws.String(e.stream),
	})

	if err != nil {
		output.timer.Start()
		return nil, err
	}
	output.timer.Reset()

	stream := &logStream{
		logStreamName:     e.stream,
		logGroupName:      e.group,
		logEvents:         make([]*cloudwatchlogs.InputLogEvent, 0, maximumLogEventsPerPut),
		nextSequenceToken: nil, // sequence token not required for a new log stream
	}
	output.streams[e.group+e.stream] = stream
	stream.updateExpiration() // initialize
	logrus.Infof("[cloudwatch %d] Created log stream %s in group %s", output.PluginInstanceID, e.stream, e.group)

	return stream, nil
}

func (output *OutputPlugin) createLogGroup(e *Event) error {
	if !output.autoCreateGroup {
		return nil
	}

	_, err := output.client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(e.group),
		Tags:         output.logGroupTags,
	})
	if err == nil {
		logrus.Infof("[cloudwatch %d] Created log group %s\n", output.PluginInstanceID, e.group)
		return output.setLogGroupRetention(e.group)
	}

	if awsErr, ok := err.(awserr.Error); !ok ||
		awsErr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
		return err
	}

	logrus.Infof("[cloudwatch %d] Log group %s already exists\n", output.PluginInstanceID, e.group)
	return output.setLogGroupRetention(e.group)
}

func (output *OutputPlugin) setLogGroupRetention(name string) error {
	if output.logGroupRetention < 1 {
		return nil
	}

	_, err := output.client.PutRetentionPolicy(&cloudwatchlogs.PutRetentionPolicyInput{
		LogGroupName:    aws.String(name),
		RetentionInDays: aws.Int64(output.logGroupRetention),
	})
	if err != nil {
		return err
	}

	logrus.Infof("[cloudwatch %d] Set retention policy on log group %s to %dd\n", output.PluginInstanceID, name, output.logGroupRetention)

	return nil
}

// Takes the byte slice and returns a string
// Also removes leading and trailing whitespace
func logString(record []byte) string {
	return strings.TrimSpace(string(record))
}

func (output *OutputPlugin) processRecord(e *Event) ([]byte, error) {
	var err error
	e.Record, err = plugins.DecodeMap(e.Record)
	if err != nil {
		logrus.Debugf("[cloudwatch %d] Failed to decode record: %v\n", output.PluginInstanceID, e.Record)
		return nil, err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	var data []byte

	if output.logKey != "" {
		log, err := plugins.LogKey(e.Record, output.logKey)
		if err != nil {
			return nil, err
		}

		data, err = plugins.EncodeLogKey(log)
	} else {
		data, err = json.Marshal(e.Record)
	}

	if err != nil {
		logrus.Debugf("[cloudwatch %d] Failed to marshal record: %v\nLog Key: %s\n", output.PluginInstanceID, e.Record, output.logKey)
		return nil, err
	}

	// append newline
	data = append(data, []byte("\n")...)

	if (len(data) + perEventBytes) > maximumBytesPerEvent {
		logrus.Warnf("[cloudwatch %d] Found record with %d bytes, truncating to 256KB, logGroup=%s, stream=%s\n",
			output.PluginInstanceID, len(data)+perEventBytes, e.group, e.stream)

		/*
		 * Find last byte of trailing unicode character via efficient byte scanning
		 * Avoids corrupting rune
		 *
		 * A unicode character may be composed of 1 - 4 bytes
		 *   bytes [11, 01, 00]xx xxxx: represent the first byte in a unicode character
		 *   byte 10xx xxxx: represent all bytes following the first byte.
		 *
		 * nextByte is the first byte that is truncated,
		 * so nextByte should be the start of a new unicode character in first byte format.
		 */
		nextByte := (maximumBytesPerEvent - len(truncatedSuffix) - perEventBytes)
		for (data[nextByte]&0xc0 == 0x80) && nextByte > 0 {
			nextByte--
		}

		data = data[:nextByte]
		data = append(data, []byte(truncatedSuffix)...)
	}

	return data, nil
}

func (output *OutputPlugin) getECSMetadata() error {
	ecsTaskMetadataEndpointV3 := os.Getenv("ECS_CONTAINER_METADATA_URI")
	var metadata TaskMetadata
	res, err := http.Get(fmt.Sprintf("%s/task", ecsTaskMetadataEndpointV3))
	if err != nil {
		return fmt.Errorf("Failed to get endpoint response: %w", err)
	}
	response, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Failed to read response '%v' from URL: %w", res, err)
	}
	res.Body.Close()

	err = json.Unmarshal(response, &metadata)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal ECS metadata '%+v': %w", metadata, err)
	}

	arnInfo, err := arn.Parse(metadata.TaskARN)
	if err != nil {
		return fmt.Errorf("Failed to parse ECS TaskARN '%s': %w", metadata.TaskARN, err)
	}
	resourceID := strings.Split(arnInfo.Resource, "/")
	taskID := resourceID[len(resourceID)-1]
	metadata.TaskID = taskID

	output.ecsMetadata = metadata
	return nil
}

// Flush sends the current buffer of records.
func (output *OutputPlugin) Flush() error {
	logrus.Debugf("[cloudwatch %d] Flush() Called", output.PluginInstanceID)

	for _, stream := range output.streams {
		if err := output.flushStream(stream); err != nil {
			return err
		}
	}

	return nil
}

func (output *OutputPlugin) flushStream(stream *logStream) error {
	output.cleanUpExpiredLogStreams() // will periodically clean up, otherwise is no-op
	return output.putLogEvents(stream)
}

func (output *OutputPlugin) putLogEvents(stream *logStream) error {
	// return in case of empty logEvents
	if len(stream.logEvents) == 0 {
		return nil
	}

	output.timer.Check()
	stream.updateExpiration()

	// Log events in a single PutLogEvents request must be in chronological order.
	sort.SliceStable(stream.logEvents, func(i, j int) bool {
		return aws.Int64Value(stream.logEvents[i].Timestamp) < aws.Int64Value(stream.logEvents[j].Timestamp)
	})
	response, err := output.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     stream.logEvents,
		LogGroupName:  aws.String(stream.logGroupName),
		LogStreamName: aws.String(stream.logStreamName),
		SequenceToken: stream.nextSequenceToken,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == cloudwatchlogs.ErrCodeDataAlreadyAcceptedException {
				// already submitted, just grab the correct sequence token
				parts := strings.Split(awsErr.Message(), " ")
				stream.nextSequenceToken = &parts[len(parts)-1]
				stream.logEvents = stream.logEvents[:0]
				stream.currentByteLength = 0
				stream.currentBatchStart = nil
				stream.currentBatchEnd = nil
				logrus.Infof("[cloudwatch %d] Encountered error %v; data already accepted, ignoring error\n", output.PluginInstanceID, awsErr)
				return nil
			} else if awsErr.Code() == cloudwatchlogs.ErrCodeInvalidSequenceTokenException {
				// sequence code is bad, grab the correct one and retry
				parts := strings.Split(awsErr.Message(), " ")
				nextSequenceToken := &parts[len(parts)-1]
				// If this is a new stream then the error will end like "The next expected sequenceToken is: null" and sequenceToken should be nil
				if strings.HasPrefix(*nextSequenceToken, "null") {
					nextSequenceToken = nil
				}
				stream.nextSequenceToken = nextSequenceToken

				return output.putLogEvents(stream)
			} else if awsErr.Code() == cloudwatchlogs.ErrCodeResourceNotFoundException {
				// a log group or a log stream should be re-created after it is deleted and then retry
				logrus.Errorf("[cloudwatch %d] Encountered error %v; detailed information: %s\n", output.PluginInstanceID, awsErr, awsErr.Message())
				if strings.Contains(awsErr.Message(), "group") {
					if err := output.createLogGroup(&Event{group: stream.logGroupName}); err != nil {
						logrus.Errorf("[cloudwatch %d] Encountered error %v\n", output.PluginInstanceID, err)
						return err
					}
				} else if strings.Contains(awsErr.Message(), "stream") {
					if _, err := output.createStream(&Event{group: stream.logGroupName, stream: stream.logStreamName}); err != nil {
						logrus.Errorf("[cloudwatch %d] Encountered error %v\n", output.PluginInstanceID, err)
						return err
					}
				}

				return fmt.Errorf("A Log group/stream did not exist, re-created it. Will retry PutLogEvents on next flush")
			} else {
				output.timer.Start()
				return err
			}
		} else {
			return err
		}
	}
	output.processRejectedEventsInfo(response)
	output.timer.Reset()
	logrus.Debugf("[cloudwatch %d] Sent %d events to CloudWatch for stream '%s' in group '%s'",
		output.PluginInstanceID, len(stream.logEvents), stream.logStreamName, stream.logGroupName)

	stream.nextSequenceToken = response.NextSequenceToken
	stream.logEvents = stream.logEvents[:0]
	stream.currentByteLength = 0
	stream.currentBatchStart = nil
	stream.currentBatchEnd = nil

	return nil
}

func (output *OutputPlugin) processRejectedEventsInfo(response *cloudwatchlogs.PutLogEventsOutput) {
	if response.RejectedLogEventsInfo != nil {
		if response.RejectedLogEventsInfo.ExpiredLogEventEndIndex != nil {
			logrus.Warnf("[cloudwatch %d] %d log events were marked as expired by CloudWatch\n", output.PluginInstanceID, aws.Int64Value(response.RejectedLogEventsInfo.ExpiredLogEventEndIndex))
		}
		if response.RejectedLogEventsInfo.TooNewLogEventStartIndex != nil {
			logrus.Warnf("[cloudwatch %d] %d log events were marked as too new by CloudWatch\n", output.PluginInstanceID, aws.Int64Value(response.RejectedLogEventsInfo.TooNewLogEventStartIndex))
		}
		if response.RejectedLogEventsInfo.TooOldLogEventEndIndex != nil {
			logrus.Warnf("[cloudwatch %d] %d log events were marked as too old by CloudWatch\n", output.PluginInstanceID, aws.Int64Value(response.RejectedLogEventsInfo.TooOldLogEventEndIndex))
		}
	}
}

// counts the effective number of bytes in the string, after
// UTF-8 normalization.  UTF-8 normalization includes replacing bytes that do
// not constitute valid UTF-8 encoded Unicode codepoints with the Unicode
// replacement codepoint U+FFFD (a 3-byte UTF-8 sequence, represented in Go as
// utf8.RuneError)
// this works because Go range will parse the string as UTF-8 runes
// copied from AWSLogs driver: https://github.com/moby/moby/commit/1e8ef386279e2e28aff199047e798fad660efbdd
func cloudwatchLen(event string) int {
	effectiveBytes := perEventBytes
	for _, rune := range event {
		effectiveBytes += utf8.RuneLen(rune)
	}
	return effectiveBytes
}

func (stream *logStream) logBatchSpan(timestamp time.Time) time.Duration {
	if stream.currentBatchStart == nil || stream.currentBatchEnd == nil {
		return 0
	}

	if stream.currentBatchStart.After(timestamp) {
		return stream.currentBatchEnd.Sub(timestamp)
	} else if stream.currentBatchEnd.Before(timestamp) {
		return timestamp.Sub(*stream.currentBatchStart)
	}

	return stream.currentBatchEnd.Sub(*stream.currentBatchStart)
}
