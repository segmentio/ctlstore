package executive

import (
	"net/url"
	"testing"

	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/stretchr/testify/assert"
)

func TestAddParameterToDSN(t *testing.T) {
	dsn := "http://foo.bar?x=y"
	updated, err := ctldb.AddParameterToDSN(dsn, "update", "newvalue")
	assert.NoError(t, err)
	parsed, err := url.Parse(updated)
	assert.NoError(t, err)
	if val := parsed.Query().Get("x"); val != "y" {
		t.Fatalf("old value should be 'y' but was '%s'", val)
	}
	if val := parsed.Query().Get("update"); val != "newvalue" {
		t.Fatalf("new value should be 'newvalue' but was '%s'", val)
	}
}
