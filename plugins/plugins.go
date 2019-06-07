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

// Package plugins contains functions that are useful across fluent bit plugins.
// This package will be imported by the CloudWatch Logs and Kinesis Data Streams plugins.
package plugins

import (
	"os"
	"strings"
	"time"

	retry "github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
)

const (
	fluentBitLogLevelEnvVar  = "FLB_LOG_LEVEL"
	sendFailureTimeoutEnvVar = "SEND_FAILURE_TIMEOUT"
)

const (
	initialInterval = 100 // milliseconds
	maxInterval     = 10  // seconds
)

// Backoff wraps github.com/cenkalti/backoff
// Wait() is called for each AWS API call that may need back off
// But backoff only occurs if StartBackoff() has previously been called
// Reset() should be called whenever backoff can end.
type Backoff struct {
	doBackoff  bool
	expBackoff *retry.ExponentialBackOff
}

// Reset ends the exponential backoff
func (b *Backoff) Reset() {
	b.doBackoff = false
	b.expBackoff.Reset()
}

// Wait enacts the exponential backoff, if StartBackoff() has been called
func (b *Backoff) Wait() {
	if b.doBackoff {
		d := b.expBackoff.NextBackOff()
		logrus.Debugf("[go plugin] In exponential backoff, waiting %v", d)
		time.Sleep(d)
	}
}

// StartBackoff begins exponential backoff
// its a no-op if backoff has already started
func (b *Backoff) StartBackoff() {
	b.doBackoff = true
}

// NewBackoff creates a new Backoff struct with default values
func NewBackoff() *Backoff {
	b := retry.NewExponentialBackOff()
	b.InitialInterval = initialInterval * time.Millisecond
	b.MaxElapsedTime = 0 // The backoff object never expires
	b.MaxInterval = maxInterval * time.Second
	return &Backoff{
		doBackoff:  false,
		expBackoff: b,
	}
}

// Timeout is a simple timeout for single-threaded programming
// (Goroutines are expensive in Cgo)
type Timeout struct {
	timeoutFunc func(time.Duration)
	duration    time.Duration
	stopTime    time.Time
	ticking     bool
	enabled     bool
}

// Start the timer
// this method has no effect if the timer has already been started
func (t *Timeout) Start() {
	if t.enabled && !t.ticking {
		t.ticking = true
		t.stopTime = time.Now().Add(t.duration)
	}
}

// Reset the timer
func (t *Timeout) Reset() {
	t.ticking = false
}

// Check the timer to see if its timed out
func (t *Timeout) Check() {
	if t.enabled && t.ticking {
		if t.stopTime.Before(time.Now()) {
			// run the timeout function
			t.timeoutFunc(t.duration)
		}
	}
}

// NewTimeout returns a new timeout object
// with a duration set from the env var
// if the env var is not set, then a timer is returned that is disabled (it doesn't do anything)
func NewTimeout(timeoutFunc func(duration time.Duration)) (*Timeout, error) {
	if os.Getenv(sendFailureTimeoutEnvVar) != "" {
		duration, err := time.ParseDuration(os.Getenv(sendFailureTimeoutEnvVar))
		if err != nil {
			return nil, err
		}
		return &Timeout{
			timeoutFunc: timeoutFunc,
			duration:    duration,
			ticking:     false,
			enabled:     true,
		}, nil
	}

	// timeout not enabled
	return &Timeout{
		timeoutFunc: timeoutFunc,
		ticking:     false,
		enabled:     false,
	}, nil
}

// SetupLogger sets up Logrus with the log level determined by the Fluent Bit Env Var
func SetupLogger() {
	logrus.SetOutput(os.Stdout)
	switch strings.ToUpper(os.Getenv(fluentBitLogLevelEnvVar)) {
	default:
		logrus.SetLevel(logrus.InfoLevel)
	case "DEBUG":
		logrus.SetLevel(logrus.DebugLevel)
	case "INFO":
		logrus.SetLevel(logrus.InfoLevel)
	case "ERROR":
		logrus.SetLevel(logrus.ErrorLevel)
	}
}

// DecodeMap prepares a record for JSON marshalling
// Any []byte will be base64 encoded when marshaled to JSON, so we must directly cast all []byte to string
func DecodeMap(record map[interface{}]interface{}) (map[interface{}]interface{}, error) {
	for k, v := range record {
		switch t := v.(type) {
		case []byte:
			// convert all byte slices to strings
			record[k] = string(t)
		case map[interface{}]interface{}:
			decoded, err := DecodeMap(t)
			if err != nil {
				return nil, err
			}
			record[k] = decoded
		case []interface{}:
			decoded, err := decodeSlice(t)
			if err != nil {
				return nil, err
			}
			record[k] = decoded
		}
	}
	return record, nil
}

// DataKeys allows users to specify a list of keys in the record which they want to be sent
// all others are discarded
func DataKeys(input string, record map[interface{}]interface{}) map[interface{}]interface{} {
	input = strings.TrimSpace(input)
	keys := strings.Split(input, ",")

	for k := range record {
		var currentKey string
		switch t := k.(type) {
		case []byte:
			currentKey = string(t)
		case string:
			currentKey = t
		default:
			logrus.Debugf("[external plugin]: Unable to determine type of key %v\n", t)
			continue
		}

		if !contains(keys, currentKey) {
			delete(record, k)
		}
	}

	return record
}

func decodeSlice(record []interface{}) ([]interface{}, error) {
	for i, v := range record {
		switch t := v.(type) {
		case []byte:
			// convert all byte slices to strings
			record[i] = string(t)
		case map[interface{}]interface{}:
			decoded, err := DecodeMap(t)
			if err != nil {
				return nil, err
			}
			record[i] = decoded
		case []interface{}:
			decoded, err := decodeSlice(t)
			if err != nil {
				return nil, err
			}
			record[i] = decoded
		}
	}
	return record, nil
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
