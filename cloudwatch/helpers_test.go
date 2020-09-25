package cloudwatch

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTagKeysToMap(t *testing.T) {
	t.Parallel()

	// Testable values. Purposely "messed up" - they should all parse out OK.
	values := " key1 =value , key2=value2, key3= value3 ,key4=, key5  = v5,,key7==value7," +
		" k8, k9,key1=value1,space key = space value"
	// The values above should return a map like this.
	expect := map[string]string{"key1": "value1", "key2": "value2", "key3": "value3",
		"key4": "", "key5": "v5", "key7": "=value7", "k8": "", "k9": "", "space key": "space value"}

	for k, v := range tagKeysToMap(values) {
		assert.Equal(t, *v, expect[k], "Tag key or value failed parser.")
	}
}

func TestParseDataMapTags(t *testing.T) {
	t.Parallel()

	template := "$(missing).$(tag).$(pam['item2']['subitem2']['more']).$(pam['item']).$(pam['item2'])." +
		"$(pam['item2']['subitem'])-$(pam['item2']['subitem55'])-$(pam['item2']['subitem2']['more'])-$(tag[1])-$(tag[6])"
	data := map[interface{}]interface{}{
		"pam": map[interface{}]interface{}{
			"item": "soup",
			"item2": map[interface{}]interface{}{"subitem": []byte("SubIt3m"),
				"subitem2": map[interface{}]interface{}{"more": "final"}},
		},
	}
	s, err := parseDataMapTags(&Event{Record: data, Tag: "syslog.0"}, []string{"syslog", "0"}, template)

	assert.Nil(t, err)
	assert.Equal(t, "missing.syslog.0.final.soup..SubIt3m-subitem55-final-0-tag6", s, "Rendered string is incorrect.")
}

func TestTruncateEvent(t *testing.T) {
	t.Parallel()

	// Make a long string.
	event := make([]byte, maximumBytesPerEvent+100)
	for i := range event {
		event[i] = 'x'
	}

	output := &OutputPlugin{PluginInstanceID: 0}
	truncated, size := output.truncateEvent(&Event{group: "group", stream: "stream"}, event)

	assert.Equal(t, maximumBytesPerEvent, len(truncated), "The event was not correctly truncated.")
	assert.Equal(t, maximumBytesPerEvent, size, "The returned byte count is invalid.")
	assert.True(t, strings.HasSuffix(truncated, truncatedSuffix), "truncation suffix missing or invalid")

	event = []byte("This is a short event.")
	truncated, size = output.truncateEvent(&Event{}, event)

	assert.Equal(t, event, []byte(truncated), "The event was truncated and should not have been.")
	assert.Equal(t, len(event), size, "The returned byte count does not match the event length.")
}
