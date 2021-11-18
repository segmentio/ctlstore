package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/segmentio/cli"
)

var cliAddFields = &cli.CommandFunc{
	Help: "Add fields to an existing table",
	Desc: unindent(`
		This command makes an HTTP request to the executive service
		to add fields to an existing table

		Example:

		add-fields --family foo --table bar --field name:string
	`),
	Func: func(ctx context.Context, config struct {
		flagBase
		flagExecutive
		flagFamily
		flagTable
		flagFields
	}) (err error) {
		executive := config.MustExecutive()
		familyName := config.MustFamily()
		tableName := config.MustTable()
		fields := config.MustFields()

		var payload struct {
			Fields [][]string `json:"fields"`
		}
		for _, field := range fields {
			payload.Fields = append(payload.Fields, []string{field.name, field.typ})
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			bail("could not marshal payload: %s", err)
		}
		url := executive + "/families/" + familyName + "/tables/" + tableName
		req, err := http.NewRequest("PUT", url, bytes.NewReader(payloadBytes))
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
			bail("One or more columns already exist")
		default:
			bailResponse(resp, "could not add fields")
		}
		return nil
	},
}
