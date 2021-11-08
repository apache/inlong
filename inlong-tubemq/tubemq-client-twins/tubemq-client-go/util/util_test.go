package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitToMap(t *testing.T) {
	source := "$msgType$=metadata_journal_log,$msgTime$=202111081911,tdbusip=10.56.15.232"
	m := SplitToMap(source, ",", "=")
	properties := make(map[string]string)
	properties["$msgType$"] = "metadata_journal_log"
	properties["$msgTime$"] = "202111081911"
	properties["tdbusip"] = "10.56.15.232"
	assert.Equal(t, properties, m)
}
