package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/segmentio/errors-go"
	"github.com/segmentio/stats/v4"
	"github.com/segmentio/stats/v4/datadog"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	cobra.OnInitialize(initGetCountConfig)
	rootCmd.AddCommand(getCountsCmd)
	useFlagCTLDBAddress(getCountsCmd)
	useFlagDataDogAddress(getCountsCmd)
	useFlagDataDogBufferSize(getCountsCmd)
	useFlagDataDogTags(getCountsCmd)
	useFlagFamily(getCountsCmd)
	useFlagLDB(getCountsCmd)
	useFlagQuiet(getCountsCmd)
	useFlagSoRAddress(getCountsCmd)
	useFlagTable(getCountsCmd)
}

func initGetCountConfig() {
	viper.SetEnvPrefix("ctlstore-cli")
	viper.BindEnv(keySoRAddress)
	viper.BindEnv(keyCTLDBAddress)
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
}

// getCountsCmd represents the get-counts command
var getCountsCmd = &cobra.Command{
	Use:   "get-counts [flags]",
	Short: "Gets the total count of records from the SoR and ctlstore.",
	Long: unindent(`
		Gets the total count of records across DBs.

		This command counts all the records in a specified table in the SoR, CTLDB,
    and LDB.

		The output of this command will be a table of DB -> Counts.
	`),
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

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
		sor, err := getSoR(cmd)
		if err != nil {
			return err
		}
		ldbPath, err := getLDB(cmd)
		if err != nil {
			return err
		}
		ldb, err := openSQLiteDB(ldbPath)
		if err != nil {
			return err
		}
		ctlDB, err := getCTLDB(cmd)
		if err != nil {
			return err
		}

		if ctlDB == nil && sor == nil && ldb == nil {
			err = errors.New("at least one db is required")
			return err
		}

		results := make(map[string]int)
		if sor != nil {
			sorCount, err := getSQLCounts(ctx, sor, tableName)
			if err != nil {
				err = errors.Errorf("error getting counts from SoR: %s", err)
				return err
			}
			results["SoR"] = sorCount
		}

		ctlStoreTableName := strings.Join([]string{familyName, tableName}, "___")
		if ldb != nil {
			ldbCount, err := getSQLCounts(ctx, ldb, ctlStoreTableName)
			if err != nil {
				err = errors.Errorf("error getting counts from LDB: %s", err)
				return err
			}
			results["LDB"] = ldbCount
		}

		if ctlDB != nil {
			ctlDBCounts, err := getSQLCounts(ctx, ctlDB, ctlStoreTableName)
			if err != nil {
				err = errors.Errorf("error getting counts from CtlDB: %s", err)
				return err
			}
			results["CtlDB"] = ctlDBCounts
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.TabIndent)
		if !quiet {
			fmt.Fprintln(w, "DB\tCounts")
			fmt.Fprintln(w, "------\t-----")
		}

		for db, count := range results {
			fmt.Fprintf(w, "%s\t%d\n", db, count)
		}
		w.Flush()

		ddAddress, ddBufferSize, tags, err := getDataDogConfig(cmd)
		if err != nil {
			err = errors.Errorf("error reading datadog config: %s", err)
			return err
		}

		if ddAddress != "" {
			stats.Register(datadog.NewClientWith(datadog.ClientConfig{Address: ddAddress, BufferSize: ddBufferSize}))
			stats.WithTags(tags...)
			defer stats.Flush()

			for db, count := range results {
				stats.Set("record-counts", count,
					stats.T("db", db),
					stats.T("family", familyName),
					stats.T("table", tableName),
				)
			}
		}

		return nil
	},
}

func getSQLCounts(ctx context.Context, db *sql.DB, table string) (int, error) {
	row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table))
	var total int
	err := row.Scan(&total)
	return total, err
}

func openSQLiteDB(path string) (*sql.DB, error) {
	if path == "/var/spool/ctlstore/ldb.db" {
		path = "file:" + path + "?_journal_mode=wal&mode=ro"
	}

	db, err := sql.Open("sqlite3", path)
	return db, err
}
