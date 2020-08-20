package cloudwatch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTagKeysToMap(t *testing.T) {
	// Testable values. Purposely "messed up" - they should all parse out OK.
	values := " key1 =value , key2=value2, key3= value3 ,key4=, key5  = v5,,key7==value7, k8, k9,key1=value1,space key = space value"
	// The values above should return a map like this.
	expect := map[string]string{"key1": "value1", "key2": "value2", "key3": "value3",
		"key4": "", "key5": "v5", "key7": "=value7", "k8": "", "k9": "", "space key": "space value"}

	for k, v := range tagKeysToMap(values) {
		assert.Equal(t, *v, expect[k], "Tag key or value failed parser.")
	}
}

func TestParseDataMapTags(t *testing.T) {
	template := "#(missing).#(tag).#(pam['item2']['subitem2']['more']).#(pam['item']).#(pam['item2'])." +
		"#(pam['item2']['subitem'])-#(pam['item2']['subitem55'])-#(pam['item2']['subitem2']['more'])-#(tag[6])"
	data := map[interface{}]interface{}{
		"pam": map[interface{}]interface{}{
			"item": "soup",
			"item2": map[interface{}]interface{}{"subitem": []byte("SubIt3m"),
				"subitem2": map[interface{}]interface{}{"more": "final"}},
		},
	}
	s, err := parseDataMapTags(data, []string{"syslog", "0"}, template)

	assert.Nil(t, err)
	assert.Equal(t, "missing.syslog.0.final.soup..SubIt3m-subitem55-final-tag6", s, "Rendered string is incorrect.")
}
