package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"text/tabwriter"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/spf13/cobra"
)

// init for tableLimitsCmd parent command
func init() {
	rootCmd.AddCommand(tableLimitsCmd)
}

var tableLimitsCmd = &cobra.Command{
	Use:   "table-limits",
	Short: "Read, update, and delete table limits",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// this command is not runnable
		return cmd.Usage()
	},
}

// init for readTableLimitsCmd
func init() {
	tableLimitsCmd.AddCommand(readTableLimitsCmd)
	useFlagExecutive(readTableLimitsCmd)
}

var readTableLimitsCmd = &cobra.Command{
	Use:   "read",
	Short: "Read all table limits",
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
		if err != nil {
			return err
		}
		url := executive + "/limits/tables"
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			bail("could not create request: %s", err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			bail("could not make request: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			bailResponse(resp, "could not read limits")
		}
		var tsl limits.TableSizeLimits
		if err := json.NewDecoder(resp.Body).Decode(&tsl); err != nil {
			bail("could not decode response: %s", err)
		}
		fmt.Printf("warn: %d bytes\n", tsl.Global.WarnSize)
		fmt.Printf("max : %d bytes\n", tsl.Global.MaxSize)
		if len(tsl.Tables) == 0 {
			return nil
		}
		fmt.Println()
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.TabIndent)
		fmt.Fprintln(w, "FAMILY\tTABLE\tWARN\tMAX")
		fmt.Fprintln(w, "------\t-----\t----\t---")
		for _, t := range tsl.Tables {
			fmt.Fprintf(w, "%s\t%s\t%d\t%d\n", t.Family, t.Table, t.WarnSize, t.MaxSize)
		}
		return w.Flush()
	},
}

// init for updateTableLimitCmd
func init() {
	tableLimitsCmd.AddCommand(updateTableLimitCmd)
	useFlagExecutive(updateTableLimitCmd)
	useFlagTable(updateTableLimitCmd)
	useFlagFamily(updateTableLimitCmd)
	useFlagMaxSize(updateTableLimitCmd)
	useFlagWarnSize(updateTableLimitCmd)
}

var updateTableLimitCmd = &cobra.Command{
	Use:   "update",
	Short: "Update a table limit",
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
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
		maxSize, err := getMaxSize(cmd)
		if err != nil {
			return err
		}
		warnSize, err := getWarnSize(cmd)
		if err != nil {
			return err
		}
		if warnSize > maxSize {
			return errors.New(keyWarnSize + " must be <= " + keyMaxSize)
		}
		if warnSize <= 0 {
			return errors.New(keyWarnSize + " must be > 0")
		}
		if maxSize <= 0 {
			return errors.New(keyMaxSize + " must be > 0")
		}
		url := executive + "/limits/tables/" + familyName + "/" + tableName
		payload := limits.SizeLimits{
			WarnSize: warnSize,
			MaxSize:  maxSize,
		}
		req, err := http.NewRequest(http.MethodPost, url, utils.NewJsonReader(payload))
		if err != nil {
			bail("could not build request: %s", err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			bail("could not make request: %s", err)
		}
		if resp.StatusCode != http.StatusOK {
			bailResponse(resp, "could not update table limit")
		}
		return nil
	},
}

// init for deleteTableLimitCmd
func init() {
	tableLimitsCmd.AddCommand(deleteTableLimitCmd)
	useFlagExecutive(deleteTableLimitCmd)
	useFlagTable(deleteTableLimitCmd)
	useFlagFamily(deleteTableLimitCmd)
}

var deleteTableLimitCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a table limit",
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
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
		url := executive + "/limits/tables/" + familyName + "/" + tableName
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			bail("could not build request: %s", err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			bail("could not make request: %s", err)
		}
		if resp.StatusCode != http.StatusOK {
			bailResponse(resp, "could not delete table limit")
		}
		return nil
	},
}
