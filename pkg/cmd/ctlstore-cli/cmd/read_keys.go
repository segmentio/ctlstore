package cmd

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/segmentio/cli"
	"github.com/segmentio/ctlstore"
)

var cliReadKeys = &cli.CommandFunc{
	Help: "read-keys [key1], [key2], ... [keyN]",
	Desc: unindent(`
		Reads a row from a local LDB

		This command reads one row given the specified family, table, and keys.
		The order of the keys must be specified in the order they appear in the
		schema.

		The output of this command will be a table of columnName -> columnValue,
		sorted by the column names.
	`),
	Func: func(ctx context.Context, config struct {
		flagBase
		flagQuiet
		flagLDBPath
		flagFamily
		flagTable
	}, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ldbPath := config.LDBPath
		familyName := config.MustFamily()
		tableName := config.MustTable()
		quiet := config.Quiet

		keys, err := getKeys(args)
		if err != nil {
			return err
		}
		reader, err := ctlstore.ReaderForPath(ldbPath)
		if err != nil {
			return fmt.Errorf("ldb reader for path: %w", err)
		}
		defer reader.Close()
		resMap := make(map[string]interface{})
		found, err := reader.GetRowByKey(ctx, resMap, familyName, tableName, keys...)
		if err != nil {
			bail("Could not read row: %s", err)
		}
		if !found {
			bail("Not found")
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.TabIndent)
		if !quiet {
			fmt.Fprintln(w, "COLUMN\tVALUE")
			fmt.Fprintln(w, "------\t-----")
		}
		var sortedResultKeys []string
		for key := range resMap {
			sortedResultKeys = append(sortedResultKeys, key)
		}
		sort.Strings(sortedResultKeys)
		for _, key := range sortedResultKeys {
			val := resMap[key]
			valFormat := "%v"
			switch val.(type) {
			case []byte:
				valFormat = "0x%x"
			}
			fmt.Fprintln(w, fmt.Sprintf("%s\t"+valFormat, key, val))
		}
		w.Flush()
		return nil
	},
}

var hexKeyRE = regexp.MustCompile(`^0x([0-9a-fA-F]*)$`)

// getKeys converts each key input into a type that can be passed
// into the reader.GetRowByKey method.  Specifically, it checks to
// see if it's a binary literal, and handles that case explicitly.
// The other types just get passed through as regular strings.
func getKeys(args []string) (res []interface{}, err error) {
	for _, arg := range args {
		key, err := parseKey(arg)
		if err != nil {
			return res, err
		}
		res = append(res, key)
	}
	return res, nil
}

func parseKey(key string) (interface{}, error) {
	parts := hexKeyRE.FindStringSubmatch(key)
	if len(parts) != 2 {
		// it's not a hex literal, just return the key itself
		return key, nil
	}
	hex, err := hex.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("could not parse '%s' as hex", parts[1])
	}
	return hex, nil
}
