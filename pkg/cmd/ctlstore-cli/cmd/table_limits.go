package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"text/tabwriter"

	"github.com/segmentio/cli"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/utils"
)

var cliTableLimits = cli.CommandSet{
	"read": &cli.CommandFunc{
		Help: "Read all table limits",
		Func: func(ctx context.Context, config struct {
			flagBase
			flagExecutive
		}) error {
			url := config.MustExecutive() + "/limits/tables"
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
	},
	"update": &cli.CommandFunc{
		Help: "Update a table limit",
		Func: func(ctx context.Context, config struct {
			flagBase
			flagExecutive
			flagFamily
			flagTable
			flagSizeLimits
		}) error {
			executive := config.MustExecutive()
			familyName := config.MustFamily()
			tableName := config.MustTable()
			maxSize := config.MustMaxSize()
			warnSize := config.MustWarnSize()
			if warnSize > maxSize {
				bail("warnSize must be <= maxSize")
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
	},
	"delete": &cli.CommandFunc{
		Help: "Delete a table limit",
		Func: func(ctx context.Context, config struct {
			flagBase
			flagExecutive
			flagFamily
			flagTable
		}) error {
			executive := config.MustExecutive()
			familyName := config.MustFamily()
			tableName := config.MustTable()
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
	},
}
