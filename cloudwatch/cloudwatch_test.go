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

func TestAddEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
		assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
		assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
	}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil)

	output := OutputPlugin{
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

func TestAddEventCreateLogGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().CreateLogGroup(gomock.Any()).Return(&cloudwatchlogs.CreateLogGroupOutput{}, nil),
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
	)

	output := OutputPlugin{
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: false,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

// Existing Log Stream that requires 2 API calls to find
func TestAddEventExistingStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeResourceAlreadyExistsException, "Log Stream already exists", fmt.Errorf("API Error"))),
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
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

func TestAddEventExistingStreamNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log group name to match")
		}).Return(nil, awserr.New(cloudwatchlogs.ErrCodeResourceAlreadyExistsException, "Log Stream already exists", fmt.Errorf("API Error"))),
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
	)

	output := OutputPlugin{
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")

}

func TestAddEventEmptyRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	output := OutputPlugin{
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logKey:          "somekey",
		logGroupCreated: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte(""),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

func TestAddEventAndFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
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
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush(testTag)
}

func TestAddEventAndFlushDataAlreadyAcceptedException(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
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
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush(testTag)

}

func TestAddEventAndFlushDataInvalidSequenceTokenException(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
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
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: true,
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush(testTag)

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

	retCode := output.AddEvent(testTag, record, start)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(testTag, record, end)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(testTag, record, before)
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")

	retCode = output.AddEvent(testTag, record, after)
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

	retCode := output.AddEvent(testTag, record, end)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(testTag, record, start)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

	retCode = output.AddEvent(testTag, record, before)
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")

	retCode = output.AddEvent(testTag, record, after)
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func TestAddEventAndEventsCountLimit(t *testing.T) {
	output := setupLimitTestOutput(t, 1)

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	now := time.Now()

	for i := 0; i < 10000; i++ {
		retCode := output.AddEvent(testTag, record, now)
		assert.Equal(t, retCode, fluentbit.FLB_OK, fmt.Sprintf("Expected return code to FLB_OK on %d iteration", i))
	}

	retCode := output.AddEvent(testTag, record, now)
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func TestAddEventAndBatchSizeLimit(t *testing.T) {
	output := setupLimitTestOutput(t, 1)

	record := map[interface{}]interface{}{
		"somekey": []byte(strings.Repeat("some value", 100)),
	}

	now := time.Now()

	for i := 0; i < 104; i++ { // 104 * 10_000 < 1_048_576
		retCode := output.AddEvent(testTag, record, now)
		assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	}

	// 105 * 10_000 > 1_048_576
	retCode := output.AddEvent(testTag, record, now.Add(time.Hour*24+time.Nanosecond))
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")
}

func setupLimitTestOutput(t *testing.T, times int) OutputPlugin {
	ctrl := gomock.NewController(t)
	mockCloudWatch := mock_cloudwatch.NewMockLogsClient(ctrl)

	gomock.InOrder(
		mockCloudWatch.EXPECT().CreateLogStream(gomock.Any()).AnyTimes().Do(func(input *cloudwatchlogs.CreateLogStreamInput) {
			assert.Equal(t, aws.StringValue(input.LogGroupName), testLogGroup, "Expected log group name to match")
			assert.Equal(t, aws.StringValue(input.LogStreamName), testLogStreamPrefix+testTag, "Expected log stream name to match")
		}).Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil),
		mockCloudWatch.EXPECT().PutLogEvents(gomock.Any()).Times(times).Return(nil, errors.New("should fail")),
	)

	return OutputPlugin{
		logGroupName:    testLogGroup,
		logStreamPrefix: testLogStreamPrefix,
		client:          mockCloudWatch,
		timer:           setupTimeout(),
		streams:         make(map[string]*logStream),
		logGroupCreated: true,
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
