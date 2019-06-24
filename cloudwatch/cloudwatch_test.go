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
	"testing"
	"time"

	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch/mock_cloudwatch"
	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/plugins"
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
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

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
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

// Existing Log Stream that requires 2 API calls to find
func TestAddEventExistingStream(t *testing.T) {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

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
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")

}

func TestAddEventExistingStreamNotFound(t *testing.T) {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

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
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_RETRY, "Expected return code to FLB_RETRY")

}

func TestAddEventAndFlush(t *testing.T) {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

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
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush(testTag)

}

func TestAddEventAndFlushDataAlreadyAcceptedException(t *testing.T) {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

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
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush(testTag)

}

func TestAddEventAndFlushDataInvalidSequenceTokenException(t *testing.T) {
	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[firehose] timeout threshold reached: Failed to send logs for %v\n", d)
		logrus.Error("[firehose] Quitting Fluent Bit")
		os.Exit(1)
	})

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
		backoff:         plugins.NewBackoff(),
		timer:           timer,
		streams:         make(map[string]*logStream),
	}

	record := map[interface{}]interface{}{
		"somekey": []byte("some value"),
	}

	retCode := output.AddEvent(testTag, record, time.Now())
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to FLB_OK")
	output.Flush(testTag)

}
