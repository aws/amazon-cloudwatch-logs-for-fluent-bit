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
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch/mock_cloudwatch"
	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	fluentbit "github.com/fluent/fluent-bit-go/output"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	testRegion          = "us-west-2"
	testLogGroup        = "my-logs"
	testLogStreamPrefix = "my-prefix"
	testTag             = "tag"
	testNextToken       = "next-token"
	testSequenceToken   = "sequence-token"
)

type configTest struct {
	name          string
	config        OutputPluginConfig
	isValidConfig bool
	expectedError string
}

var (
	configValidationTestCases = []configTest{
		{
			name: "ValidConfiguration",
			config: OutputPluginConfig{
				Region:          testRegion,
				LogGroupName:    testLogGroup,
				LogStreamPrefix: testLogStreamPrefix,
			},
			isValidConfig: true,
			expectedError: "",
		},
		{
			name: "MissingRegion",
			config: OutputPluginConfig{
				LogGroupName:    testLogGroup,
				LogStreamPrefix: testLogStreamPrefix,
			},
			isValidConfig: false,
			expectedError: "region is a required parameter",
		},
		{
			name: "MissingLogGroup",
			config: OutputPluginConfig{
				Region:          testRegion,
				LogStreamPrefix: testLogStreamPrefix,
			},
			isValidConfig: false,
			expectedError: "log_group_name is a required parameter",
		},
		{
			name: "OnlyLogStreamNameProvided",
			config: OutputPluginConfig{
				Region:        testRegion,
				LogGroupName:  testLogGroup,
				LogStreamName: "testLogStream",
			},
			isValidConfig: true,
		},
		{
			name: "OnlyLogStreamPrefixProvided",
			config: OutputPluginConfig{
				Region:          testRegion,
				LogGroupName:    testLogGroup,
				LogStreamPrefix: testLogStreamPrefix,
			},
			isValidConfig: true,
		},
		{
			name: "LogStreamAndPrefixBothProvided",
			config: OutputPluginConfig{
				Region:          testRegion,
				LogGroupName:    testLogGroup,
				LogStreamName:   "testLogStream",
				LogStreamPrefix: testLogStreamPrefix,
			},
			isValidConfig: false,
			expectedError: "either log_stream_name or log_stream_prefix can be configured. They cannot be provided together",
		},
		{
			name: "LogStreamAndPrefixBothMissing",
			config: OutputPluginConfig{
				Region:       testRegion,
				LogGroupName: testLogGroup,
			},
			isValidConfig: false,
			expectedError: "log_stream_name or log_stream_prefix is required",
		},
	}
)

// helper function to make a log stream/log group name template from a string.
func testTemplate(template string) *fastTemplate {
	t, _ := newTemplate(template)
	return t
}

func TestAddEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
}

func TestTruncateLargeLogEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": make([]byte, 256*1024+100),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	actualData, err := output.processRecord(&Event{TS: time.Now(), Tag: testTag, Record: record})

	if err != nil {
		logrus.Debugf("[cloudwatch %d] Failed to process record: %v\n", output.PluginInstanceID, record)
	}

	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.Len(t, actualData, 256*1024-26, "Expected length is 256*1024-26")
}

func TestTruncateLargeLogEventWithSpecialCharacterOneTrailingFragments(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	var b bytes.Buffer
	for i := 0; i < 262095; i++ {
		b.WriteString("x")
	}
	b.WriteString("𒁈zrgchimqigtm")

	record := map[interface{}]interface{}{
		"key": b.String(),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	actualData, err := output.processRecord(&Event{TS: time.Now(), Tag: testTag, Record: record})

	if err != nil {
		logrus.Debugf("[cloudwatch %d] Failed to process record: %v\n", output.PluginInstanceID, record)
	}

	/* invalid characters will be expanded when sent as request */
	actualDataString := logString(actualData)
	actualDataString = fmt.Sprintf("%q", actualDataString) /* converts: <invalid> -> \x<hex> */

	exampleWorkingData := "{\"key\":\"x\"}"
	addedLength := len(fmt.Sprintf("%q", exampleWorkingData)) - len(exampleWorkingData)

	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.LessOrEqual(t, len(actualDataString), 256*1024-26+addedLength, "Expected length to be less than or equal to 256*1024-26")
}

func TestTruncateLargeLogEventWithSpecialCharacterTwoTrailingFragments(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)
	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	var b bytes.Buffer
	for i := 0; i < 262094; i++ {
		b.WriteString("x")
	}
	b.WriteString("𒁈zrgchimqigtm")

	record := map[interface{}]interface{}{
		"key": b.String(),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	actualData, err := output.processRecord(&Event{TS: time.Now(), Tag: testTag, Record: record})

	if err != nil {
		logrus.Debugf("[cloudwatch %d] Failed to process record: %v\n", output.PluginInstanceID, record)
	}

	/* invalid characters will be expanded when sent as request */
	actualDataString := logString(actualData)
	actualDataString = fmt.Sprintf("%q", actualDataString) /* converts: <invalid> -> \x<hex> */

	exampleWorkingData := "{\"key\":\"x\"}"
	addedLength := len(fmt.Sprintf("%q", exampleWorkingData)) - len(exampleWorkingData)

	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.LessOrEqual(t, len(actualDataString), 256*1024-26+addedLength, "Expected length to be less than or equal to 256*1024-26")
}

func TestTruncateLargeLogEventWithSpecialCharacterThreeTrailingFragments(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)
	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	var b bytes.Buffer
	for i := 0; i < 262093; i++ {
		b.WriteString("x")
	}
	b.WriteString("𒁈zrgchimqigtm")

	record := map[interface{}]interface{}{
		"key": b.String(),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	actualData, err := output.processRecord(&Event{TS: time.Now(), Tag: testTag, Record: record})

	if err != nil {
		logrus.Debugf("[cloudwatch %d] Failed to process record: %v\n", output.PluginInstanceID, record)
	}

	/* invalid characters will be expanded when sent as request */
	actualDataString := logString(actualData)
	actualDataString = fmt.Sprintf("%q", actualDataString) /* converts: <invalid> -> \x<hex> */

	exampleWorkingData := "{\"key\":\"x\"}"
	addedLength := len(fmt.Sprintf("%q", exampleWorkingData)) - len(exampleWorkingData)

	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.LessOrEqual(t, len(actualDataString), 256*1024-26+addedLength, "Expected length to be less than or equal to 256*1024-26")
}

func TestAddEventCreateLogGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().CreateLogGroup(gomock.Any()).Return(&cloudwatchlogs.CreateLogGroupOutput{}, nil),
		mockCloudWatch.EXPECT().PutRetentionPolicy(gomock.Any()).Return(&cloudwatchlogs.PutRetentionPolicyOutput{}, nil),
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:      testTemplate(testLogGroup),
		logStreamPrefix:   testLogStreamPrefix,
		client:            mockCloudWatch,
		timer:             setupTimeout(),
		streams:           make(map[string]*logStream),
		groups:            make(map[string]struct{}),
		logGroupRetention: 14,
		autoCreateGroup:   true,
		autoCreateStream:  true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

// Existing Log Stream that requires 2 API calls to find
func TestAddEventExistingStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamNamePrefix), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{
			LogStreams: []*cloudwatchlogs.LogStream{
				&cloudwatchlogs.LogStream{
					LogStreamName: aws.String("wrong stream"),
				},
			},
			NextToken: aws.String(testNextToken),
		}, nil),
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamNamePrefix), testLogStreamPrefix+testTag, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.NextToken), testNextToken, "Expected next token to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{
			LogStreams: []*cloudwatchlogs.LogStream{
				&cloudwatchlogs.LogStream{
					LogStreamName: aws.String(testLogStreamPrefix + testTag),
				},
			},
			NextToken: aws.String(testNextToken),
		}, nil),
	)

	output := OutputPlugin{
		logGroupName:    testTemplate(testLogGroup),
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		groups:          map[string]struct{}{testLogGroup: {}},
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

func TestAddEventDescribeStreamsException(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
	}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeResourceNotFoundException, "The specified log group does not exist.", fmt.Errorf("API Error")))

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_OK")
}

func TestAddEventAutoCreateDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
		assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
	}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil)
	mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
	}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil).Times(0)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: false,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func TestAddEventExistingStreamNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamNamePrefix), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{
			LogStreams: []*cloudwatchlogs.LogStream{
				&cloudwatchlogs.LogStream{
					LogStreamName: aws.String("wrong stream"),
				},
			},
			NextToken: aws.String(testNextToken),
		}, nil),
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamNamePrefix), testLogStreamPrefix+testTag, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.NextToken), testNextToken, "Expected next token to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{
			LogStreams: []*cloudwatchlogs.LogStream{
				&cloudwatchlogs.LogStream{
					LogStreamName: aws.String("another wrong stream"),
				},
			},
		}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeResourceAlreadyExistsException, "Log Stream already exists", fmt.Errorf("API Error"))),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")

}

func TestAddEventEmptyRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	output := OutputPlugin{
		logGroupName:    testTemplate(testLogGroup),
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logKey:          "somekey",
		groups:          map[string]struct{}{testLogGroup: {}},
	}

	record := map[interface{}]interface{}{
		"somekey": []byte(""),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

func TestAddEventAndFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.PutLogEventsOutput{
			NextSequenceToken: aws.String("token"),
		}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush()
}

func TestPutLogEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	output := OutputPlugin{
		logGroupName:    testTemplate(testLogGroup),
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logKey:          "somekey",
		groups:          map[string]struct{}{testLogGroup: {}},
	}

	stream := &logStream{}
	err := output.putLogEvents(stream)
	assert.Nil(t, err)
}

func TestSetGroupStreamNames(t *testing.T) {
	record := map[interface{}]interface{}{
		"ident": "cron",
		"msg":   "my cool log message",
		"details": map[interface{}]interface{}{
			"region": "us-west-2",
			"az":     "a",
		},
	}

	e := &Event{Tag: "syslog.0", Record: record}

	// Test against non-template name.
	output := OutputPlugin{
		logStreamName:        testTemplate("/aws/ecs/test-stream-name"),
		logGroupName:         testTemplate(""),
		defaultLogGroupName:  "fluentbit-default",
		defaultLogStreamName: "/fluentbit-default",
	}

	output.setGroupStreamNames(e)
	assert.Equal(t, "/aws/ecs/test-stream-name", e.stream,
		"The provided stream name must be returned exactly, without modifications.")

	output.logStreamName = testTemplate("")
	output.setGroupStreamNames(e)
	assert.Equal(t, output.defaultLogStreamName, e.stream,
		"The default stream name must be set when no stream name is provided.")

	// Test against a simple log stream prefix.
	output.logStreamPrefix = "/aws/ecs/test-stream-prefix/"
	output.setGroupStreamNames(e)
	assert.Equal(t, output.logStreamPrefix+"syslog.0", e.stream,
		"The provided stream prefix must be prefixed to the provided tag name.")

	// Test replacing items from template variables.
	output.logStreamPrefix = ""
	output.logStreamName = testTemplate("/aws/ecs/$(tag[0])/$(tag[1])/$(details['region'])/$(details['az'])/$(ident)")
	output.setGroupStreamNames(e)
	assert.Equal(t, "/aws/ecs/syslog/0/us-west-2/a/cron", e.stream,
		"The stream name template was not correctly parsed.")
	assert.Equal(t, output.defaultLogGroupName, e.group,
		"The default log group name must be set when no log group is provided.")

	// Test another bad template ] missing.
	output.logStreamName = testTemplate("/aws/ecs/$(details['region')")
	output.setGroupStreamNames(e)
	assert.Equal(t, "/aws/ecs/['region'", e.stream,
		"The provided stream name must match when the tag is incomplete.")

	// Make sure we get default group and stream names when their variables cannot be parsed.
	output.logStreamName = testTemplate("/aws/ecs/$(details['activity'])")
	output.logGroupName = testTemplate("$(details['activity'])")
	output.setGroupStreamNames(e)
	assert.Equal(t, output.defaultLogStreamName, e.stream,
		"The default stream name must return when elements are missing.")
	assert.Equal(t, output.defaultLogGroupName, e.group,
		"The default group name must return when elements are missing.")

	// Test that log stream and log group names get truncated to the maximum allowed.
	b := make([]byte, maxGroupStreamLength*2)
	for i := range b { // make a string twice the max
		b[i] = '_'
	}

	ident := string(b)
	assert.True(t, len(ident) > maxGroupStreamLength, "test string creation failed")

	e.Record = map[interface{}]interface{}{"ident": ident} // set the long string into our record.
	output.logStreamName = testTemplate("/aws/ecs/$(ident)")
	output.logGroupName = testTemplate("/aws/ecs/$(ident)")

	output.setGroupStreamNames(e)
	assert.Equal(t, maxGroupStreamLength, len(e.stream), "the stream name should be truncated to the maximum size")
	assert.Equal(t, maxGroupStreamLength, len(e.group), "the group name should be truncated to the maximum size")
	assert.Equal(t, "/aws/ecs/"+string(b[:maxGroupStreamLength-len("/aws/ecs/")]),
		e.stream, "the stream name was incorrectly truncated")
	assert.Equal(t, "/aws/ecs/"+string(b[:maxGroupStreamLength-len("/aws/ecs/")]),
		e.group, "the group name was incorrectly truncated")
}

func TestAddEventAndFlushDataAlreadyAcceptedException(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeDataAlreadyAcceptedException, "Data already accepted; The next expected sequenceToken is: "+testSequenceToken, fmt.Errorf("API Error"))),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush()
}

func TestAddEventAndFlushDataInvalidSequenceTokenException(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeInvalidSequenceTokenException, "The given sequenceToken is invalid; The next expected sequenceToken is: "+testSequenceToken, fmt.Errorf("API Error"))),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
			assert.Equal(t, aws.StringValue(input.SequenceToken), testSequenceToken, "Expected sequence token to match response from previous error")
		}).Return(&cloudwatchlogs.PutLogEventsOutput{
			NextSequenceToken: aws.String("token"),
		}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush()
}

func TestAddEventAndFlushDataInvalidSequenceTokenNextNullException(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeInvalidSequenceTokenException, "The given sequenceToken is invalid; The next expected sequenceToken is: null", fmt.Errorf("API Error"))),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
			assert.Nil(t, input.SequenceToken, "Expected sequence token to be nil")
		}).Return(&cloudwatchlogs.PutLogEventsOutput{
			NextSequenceToken: aws.String("token"),
		}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush()
}

func TestAddEventAndDataResourceNotFoundExceptionWithNoLogGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeResourceNotFoundException, "The specified log group does not exist.", fmt.Errorf("API Error"))),
		mockCloudWatch.EXPECT().CreateLogGroup(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogGroupInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogGroupOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
}

func TestAddEventAndDataResourceNotFoundExceptionWithNoLogStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Do(func(input *cloudwatchlogs.PutLogEventsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeResourceNotFoundException, "The specified log stream does not exist.", fmt.Errorf("API Error"))),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(&Event{TS: time.Now(), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
}

func TestAddEventAndBatchSpanLimit(t *testing.T) {
	output := setupLimitTestOutput(t, 2)

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	before := time.Now()
	start := before.Add(time.Nanosecond)
	end := start.Add(time.Hour*24 - time.Nanosecond)
	after := start.Add(time.Hour * 24)

	retCode := output.AddEvent(&Event{TS: start, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(&Event{TS: end, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(&Event{TS: before, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")

	retCode = output.AddEvent(&Event{TS: after, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func TestAddEventAndBatchSpanLimitOnReverseOrder(t *testing.T) {
	output := setupLimitTestOutput(t, 2)

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	before := time.Now()
	start := before.Add(time.Nanosecond)
	end := start.Add(time.Hour*24 - time.Nanosecond)
	after := start.Add(time.Hour * 24)

	retCode := output.AddEvent(&Event{TS: end, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(&Event{TS: start, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(&Event{TS: before, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")

	retCode = output.AddEvent(&Event{TS: after, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func TestAddEventAndEventsCountLimit(t *testing.T) {
	output := setupLimitTestOutput(t, 1)

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	now := time.Now()

	for i := 0; i < 10000; i++ {
		retCode := output.AddEvent(&Event{TS: now, Tag: testTag, Record: record})
		assert.Equal(t, retCode, fluentbit.FLB_OK, fmt.Sprintf("Expected return code to FLB_OK on %d iteration", i))
	}
	retCode := output.AddEvent(&Event{TS: now, Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func TestAddEventAndBatchSizeLimit(t *testing.T) {
	output := setupLimitTestOutput(t, 1)

	record := map[interface{}]interface{}{
		"somekey": []byte(strings.Repeat("some value", 100)),
	}

	now := time.Now()

	for i := 0; i < 104; i++ { // 104 * 10_000 < 1_048_576
		retCode := output.AddEvent(&Event{TS: now, Tag: testTag, Record: record})
		assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	}

	// 105 * 10_000 > 1_048_576
	retCode := output.AddEvent(&Event{TS: now.Add(time.Hour*24 + time.Nanosecond), Tag: testTag, Record: record})
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func setupLimitTestOutput(t *testing.T, times int) OutputPlugin {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().DescribeLogStreams(gomock.Any()).Do(func(input *cloudwatchlogs.DescribeLogStreamsInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		}).Return(&cloudwatchlogs.DescribeLogStreamsOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).AnyTimes().Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Times(times).Return(nil, errors.New("should fail")),
	)

	return OutputPlugin{
		logGroupName:     testTemplate(testLogGroup),
		logStreamPrefix:  testLogStreamPrefix,
		client:           mockCloudWatch,
		timer:            setupTimeout(),
		streams:          make(map[string]*logStream),
		groups:           map[string]struct{}{testLogGroup: {}},
		autoCreateStream: true,
	}
}

func setupTimeout() *plugins.Timeout {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})
	return timer
}

func TestValidate(t *testing.T) {
	for _, test := range configValidationTestCases {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()

			if test.isValidConfig {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, err.Error(), test.expectedError)
			}
		})
	}
}
