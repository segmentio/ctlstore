package utils

import (
	"bytes"
	"encoding/json"
	"io"
)

// JsonReader is a convenience type that is constructed with a
// type to be serialized using newJsonReader.  it implements
// io.Reader and writes JSON bytes to the client.  Useful for
// supplying a reader for the body of an http request. This
// allows the client to omit the extra step of encoding a struct
// into a byte slice and then passing a bytes.NewReader(b) to
// something expecting that reader.
type JsonReader struct {
	reader io.Reader
	err    error
}

func (r *JsonReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return -1, err
	}
	return r.reader.Read(p)
}

func NewJsonReader(val interface{}) *JsonReader {
	b, err := json.Marshal(val)
	return &JsonReader{
		reader: bytes.NewReader(b),
		err:    err,
	}
}
