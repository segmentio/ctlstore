package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/spf13/cobra"
)

// init for writerLimitsCmd parent command
func init() {
	rootCmd.AddCommand(writerLimitsCmd)
}

var writerLimitsCmd = &cobra.Command{
	Use:   "writer-limits",
	Short: "Read, update, and delete writer rate limits",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// this command is not runnable
		return cmd.Usage()
	},
}

// init for readWriterLimitsCmd
func init() {
	writerLimitsCmd.AddCommand(readWriterLimitsCmd)
	useFlagExecutive(readWriterLimitsCmd)
}

var readWriterLimitsCmd = &cobra.Command{
	Use:   "read",
	Short: "Read all writer limits",
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
		if err != nil {
			return err
		}
		url := executive + "/limits/writers"
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
		var wrl limits.WriterRateLimits
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			bail("could not read response: %s", err)
		}
		if err := json.Unmarshal(b, &wrl); err != nil {
			bail("could not decode response: %s", err)
		}
		fmt.Println("default:", wrl.Global)
		if len(wrl.Writers) == 0 {
			return nil
		}
		fmt.Println()
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.TabIndent)
		fmt.Fprintln(w, "WRITER\tLIMIT")
		fmt.Fprintln(w, "------\t-----")
		for _, t := range wrl.Writers {
			fmt.Fprintf(w, "%s\t%s\n", t.Writer, t.RateLimit)
		}
		return w.Flush()
	},
}

// init for updateWriterLimitsCmd
func init() {
	writerLimitsCmd.AddCommand(updateWriterLimitsCmd)
	useFlagExecutive(updateWriterLimitsCmd)
	useFlagRowsPerMinute(updateWriterLimitsCmd)
	useFlagWriter(updateWriterLimitsCmd)
}

var updateWriterLimitsCmd = &cobra.Command{
	Use:   "update",
	Short: "Add or update a writer limit",
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
		if err != nil {
			return err
		}
		writer, err := getWriter(cmd)
		if err != nil {
			return err
		}
		rowsPerMinute, err := getRowsPerMinute(cmd)
		if err != nil {
			return err
		}
		url := executive + "/limits/writers/" + writer
		payload := limits.RateLimit{
			Amount: rowsPerMinute,
			Period: time.Minute,
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
			bailResponse(resp, "could not update writer limit")
		}
		return nil
	},
}

// init for deleteWriterLimitsCmd
func init() {
	writerLimitsCmd.AddCommand(deleteWriterLimitsCmd)
	useFlagExecutive(deleteWriterLimitsCmd)
	useFlagWriter(deleteWriterLimitsCmd)
}

var deleteWriterLimitsCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a writer limit",
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
		if err != nil {
			return err
		}
		writer, err := getWriter(cmd)
		if err != nil {
			return err
		}
		url := executive + "/limits/writers/" + writer
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			bail("could not build request: %s", err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			bail("could not make request: %s", err)
		}
		if resp.StatusCode != http.StatusOK {
			bailResponse(resp, "could not delete writer limit")
		}
		return nil
	},
}
