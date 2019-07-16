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

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(readKeysCmd)
	useFlagFamily(readKeysCmd)
	useFlagTable(readKeysCmd)
	useFlagLDB(readKeysCmd)
	useFlagQuiet(readKeysCmd)
}

var hexKeyRE = regexp.MustCompile(`^0x([0-9a-fA-F]*)$`)

// readKeysCmd represents the read-keys command
var readKeysCmd = &cobra.Command{
	Use:   "read-keys [key1], [key2], ... [keyN]",
	Short: "Reads a row from a local LDB",
	Long: unindent(`
		Reads a row from a local LDB

		This command reads one row given the specified family, table, and keys.
		The order of the keys must be specified in the order they appear in the
		schema.

		The output of this command will be a table of columnName -> columnValue,
		sorted by the column names.
	`),
	Args: cobra.MinimumNArgs(1), // at least one key must be supplied
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ldbPath, err := getLDB(cmd)
		if err != nil {
			return err
		}
		familyName, err := getFamilyName(cmd)
		if err != nil {
			return err
		}
		tableName, err := getTableName(cmd)
		if err != nil {
			return err
		}
		quiet, err := cmd.Flags().GetBool(keyQuiet)
		if err != nil {
			return err
		}
		keys, err := getKeys(args)
		if err != nil {
			return err
		}
		reader, err := ctlstore.ReaderForPath(ldbPath)
		if err != nil {
			return errors.Wrap(err, "ldb reader for path")
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
		return nil, errors.Errorf("could not parse '%s' as hex", parts[1])
	}
	return hex, nil
}
