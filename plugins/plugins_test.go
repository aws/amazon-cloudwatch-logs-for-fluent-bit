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

package plugins

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeMap(t *testing.T) {
	sliceVal := []interface{}{
		[]byte("Seattle is"),
		[]byte("rainy"),
	}
	innerMap := map[interface{}]interface{}{
		"clyde":       []byte("best dog"),
		"slice_value": sliceVal,
	}
	record := map[interface{}]interface{}{
		"somekey":               []byte("some value"),
		"key-with-nested-value": innerMap,
	}

	var err error
	record, err = DecodeMap(record)

	assert.NoError(t, err, "Unexpected error calling DecodeMap")

	assertTypeIsString(t, record["somekey"])
	assertTypeIsString(t, innerMap["clyde"])
	assertTypeIsString(t, sliceVal[0])
	assertTypeIsString(t, sliceVal[1])

}

func assertTypeIsString(t *testing.T, val interface{}) {
	_, ok := val.(string)
	assert.True(t, ok, "Expected value to be a string after call to DecodeMap")
}

func TestDataKeys(t *testing.T) {
	record := map[interface{}]interface{}{
		"this":         "is a test",
		"this is only": "a test",
		"dumpling":     "is a dog",
		"pudding":      "is a dog",
		"sushi":        "is a dog",
		"why do":       "people name their dogs after food...",
	}

	record = DataKeys("dumpling,pudding", record)

	assert.Len(t, record, 2, "Expected record to contain 2 keys")
	assert.Equal(t, record["pudding"], "is a dog", "Expected data key to have correct value")
	assert.Equal(t, record["dumpling"], "is a dog", "Expected data key to have correct value")
}
