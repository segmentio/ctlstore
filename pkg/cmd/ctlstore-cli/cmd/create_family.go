package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/segmentio/cli"
)

var cliCreateFamily = &cli.CommandFunc{
	Help: "Create a new table family",
	Desc: unindent(fmt.Sprintf(`
		Create a new table family

		This command makes an HTTP request to the executive service
		to create a new table family. 

		Example:
		
		%s create-family foo
	`, filepath.Base(os.Args[0]))),
	Func: func(ctx context.Context, config struct {
		flagBase
		flagExecutive
	}, args []string) (err error) {
		if len(args) != 1 {
			bail("Family required")
		}
		executive := config.MustExecutive()
		familyName := args[0]
		url := executive + "/families/" + familyName
		req, err := http.NewRequest("POST", url, nil)
		if err != nil {
			bail("could not create request: %s", err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			bail("could not make request: %s", err)
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
		case http.StatusConflict:
			fmt.Println("Family already exists")
		default:
			bailResponse(resp, "could not create family '%s'", familyName)
		}
		return nil
	},
}
