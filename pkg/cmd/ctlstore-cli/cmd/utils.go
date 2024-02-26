package cmd

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// field represents a table field. it has a name and a type.
type field struct {
	name string
	typ  string
}

var (
	httpClient = &http.Client{}
)

// bailResponse is similar to bail, but includes details about a failed
// http.Response.
func bailResponse(response *http.Response, msg string, args ...interface{}) {
	msg = fmt.Sprintf(msg, args...)
	// ok to ignore error here
	b, _ := io.ReadAll(response.Body)
	respMsg := fmt.Sprintf("server returned [%d]: %s", response.StatusCode, b)
	fmt.Fprintln(os.Stderr, fmt.Sprintf("%s: %s", msg, respMsg))
	os.Exit(1)
}

// bail prints a message to stderr and exits with status=1
func bail(msg string, args ...interface{}) {
	msg = fmt.Sprintf(msg, args...)
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

func normalizeURL(val string) string {
	if !strings.HasPrefix(val, "http://") && !strings.HasPrefix(val, "https://") {
		val = "http://" + val
	}
	if _, err := url.Parse(val); err != nil {
		bail("invalid url: %v", err)
	}
	return val
}

// unindent formats long help text before it's printed to the console.
// it's helpful to indent multiline strings to make it look nice in the
// code, but you don't want those indents to make their way to the
// console output.
func unindent(str string) string {
	str = strings.TrimSpace(str)
	out := new(bytes.Buffer)
	for _, line := range strings.Split(str, "\n") {
		out.WriteString(strings.TrimSpace(line) + "\n")
	}
	return out.String()
}
