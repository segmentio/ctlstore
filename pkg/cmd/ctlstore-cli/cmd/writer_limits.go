package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/segmentio/cli"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/utils"
)

var cliWriterLimits = &cli.CommandSet{
	"read": &cli.CommandFunc{
		Help: "Read all writer limits",
		Func: func(ctx context.Context, config struct {
			flagBase
			flagExecutive
		}) error {
			executive := config.MustExecutive()
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
			b, err := io.ReadAll(resp.Body)
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
	},
	"update": &cli.CommandFunc{
		Help: "Add or update a writer limit",
		Func: func(ctx context.Context, config struct {
			flagBase
			flagExecutive
			flagRowsPerMinute
			flagWriter
		}) error {
			executive := config.MustExecutive()
			writer := config.MustWriter()
			rowsPerMinute := config.RowsPerMinute
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
	},
	"delete": &cli.CommandFunc{
		Help: "Delete a writer limit",
		Func: func(ctx context.Context, config struct {
			flagBase
			flagExecutive
			flagWriter
		}) error {
			executive := config.MustExecutive()
			writer := config.MustWriter()
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
	},
}
