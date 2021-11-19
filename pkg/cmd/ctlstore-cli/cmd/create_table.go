package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/segmentio/cli"
)

var cliCreateTable = &cli.CommandFunc{
	Help: "Create a new table",
	Desc: unindent(`
		Create a new table

		This command makes an HTTP request to the executive service
		to create a new table. 

		Example:

		create-table --family foo --field name:string --field foo:integer --key-field name testtable

		Resulting schema:

		CREATE TABLE foo___testtable (name VARCHAR(191), foo INTEGER, PRIMARY KEY(name));
	`),
	Func: func(ctx context.Context, config struct {
		flagBase
		flagExecutive
		flagFamily
		flagFields
		flagKeyFields
	}, args []string) error {
		executive := config.MustExecutive()
		familyName := config.MustFamily()
		fields := config.MustFields()
		keyFields := config.MustKeyFields()

		tableName := args[0]

		// todo: dedupe this declaration
		var payload struct {
			Fields    [][]string `json:"fields"`
			KeyFields []string   `json:"keyFields"`
		}
		for _, field := range fields {
			payload.Fields = append(payload.Fields, []string{field.name, field.typ})
		}
		for _, keyField := range keyFields {
			payload.KeyFields = append(payload.KeyFields, keyField)
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			bail("could not marshal payload: %s", err)
		}
		url := executive + "/families/" + familyName + "/tables/" + tableName
		req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
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
			fmt.Println("Table already exists")
		default:
			bailResponse(resp, "could not create table '%s'", tableName)
		}
		return nil
	},
}
