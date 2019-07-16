package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/segmentio/stats"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// field represents a table field. it has a name and a type.
type field struct {
	name string
	typ  string
}

var (
	httpClient = &http.Client{}
)

func errFlagRequired(flag string) error {
	return errors.New(flag + " flag required")
}

// bailResponse is similar to bail, but includes details about a failed
// http.Response.
func bailResponse(response *http.Response, msg string, args ...interface{}) {
	msg = fmt.Sprintf(msg, args...)
	// ok to ignore error here
	b, _ := ioutil.ReadAll(response.Body)
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

func getKeyFields(cmd *cobra.Command) ([]string, error) {
	fields, err := cmd.Flags().GetStringArray(keyKeyFields)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, errors.New("at least one key field required")
	}
	return fields, nil
}

func getFields(cmd *cobra.Command) ([]field, error) {
	vals, err := cmd.Flags().GetStringArray(keyFields)
	if err != nil {
		return nil, err
	}
	res := make([]field, 0, len(vals))
	for _, val := range vals {
		parts := strings.Split(val, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid field: %s", val)
		}
		res = append(res, field{
			name: parts[0],
			typ:  parts[1],
		})
	}
	return res, nil
}

func getTableName(cmd *cobra.Command) (string, error) {
	return getRequiredString(cmd, keyTable)
}

func getFamilyName(cmd *cobra.Command) (string, error) {
	return getRequiredString(cmd, keyFamily)
}

func getMaxSize(cmd *cobra.Command) (int64, error) {
	return getRequiredInt64(cmd, keyMaxSize)
}

func getWarnSize(cmd *cobra.Command) (int64, error) {
	return getRequiredInt64(cmd, keyWarnSize)
}

func getWriter(cmd *cobra.Command) (string, error) {
	return getRequiredString(cmd, keyWriter)
}

func getRequiredInt64(cmd *cobra.Command, key string) (int64, error) {
	val, err := cmd.Flags().GetInt64(key)
	if err != nil {
		return 0, err
	}
	if val == 0 {
		return 0, errFlagRequired(key)
	}
	return val, nil
}

func getRequiredString(cmd *cobra.Command, key string) (string, error) {
	val, err := cmd.Flags().GetString(key)
	if err != nil {
		return "", err
	}
	if val == "" {
		return "", errFlagRequired(key)
	}
	return val, nil
}

// getLDB gets the value of the ldb flag and also ensures
// that the file exists. it returns an error for flag
// errors, but bails if the ldb does not exist. this is
// to control the behavior of the output to the user.
func getLDB(cmd *cobra.Command) (string, error) {
	ldbPath, err := cmd.Flags().GetString(keyLDB)
	if err != nil {
		return "", err
	}
	_, err = os.Stat(ldbPath)
	if os.IsNotExist(err) {
		bail("Could not find LDB: %s", err)
	}
	return ldbPath, nil
}

// getExecutive gets the executive location for the specified
// command, and ensures that it is a properly formed URL.
func getExecutive(cmd *cobra.Command) (string, error) {
	executive, err := cmd.Flags().GetString(keyExecutiveLocation)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(executive, "http://") {
		executive = "http://" + executive
	}
	if _, err := url.Parse(executive); err != nil {
		return "", errors.Wrap(err, "invalid url")
	}
	return executive, nil
}

func getRowsPerMinute(cmd *cobra.Command) (int64, error) {
	wpm, err := cmd.Flags().GetInt64(keyRowsPerMinute)
	if err != nil {
		return 0, err
	}
	return wpm, nil
}

// getSoR gets the value of the sor-address. first check
// if there is an env variable set for it then check the
// flag.
func getSoR(cmd *cobra.Command) (*sql.DB, error) {
	var err error

	sorAddress := viper.GetString(keySoRAddress)
	if sorAddress == "" {
		sorAddress, err = cmd.Flags().GetString(keySoRAddress)
		if err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("mysql", sorAddress)
	if err != nil {
		bail("Could not connect to SoR: %s", err)
	}
	return db, nil
}

// getCTLDB gets the value of the ctldb-address. first check
// if there is an env variable set for it then check the flag.
func getCTLDB(cmd *cobra.Command) (*sql.DB, error) {
	var err error

	ctlDBAddress := viper.GetString(keyCTLDBAddress)
	if ctlDBAddress == "" {
		ctlDBAddress, err = cmd.Flags().GetString(keyCTLDBAddress)
		if err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("mysql", ctlDBAddress)
	if err != nil {
		bail("Could not connect to CTLDB: %s", err)
	}
	return db, nil
}

func getDataDogConfig(cmd *cobra.Command) (string, int, []stats.Tag, error) {
	address, err := cmd.Flags().GetString(keyDataDogAddress)
	if err != nil {
		return "", 0, nil, err
	}

	bufferSize, err := cmd.Flags().GetInt(keyDataDogBufferSize)
	if err != nil {
		return "", 0, nil, err
	}
	if bufferSize == 0 && address != "" {
		bufferSize = 1024
	}

	tags, err := cmd.Flags().GetStringSlice(keyDataDogTags)
	ddTags := make([]stats.Tag, len(tags))
	for _, tag := range tags {
		t := strings.Split(tag, ":")
		ddTag := stats.Tag{
			Name:  t[0],
			Value: t[1],
		}
		ddTags = append(ddTags, ddTag)
	}
	return address, bufferSize, ddTags, err
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
