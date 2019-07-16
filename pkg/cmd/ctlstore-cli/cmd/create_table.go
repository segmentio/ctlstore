package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(createTableCmd)
	useFlagExecutive(createTableCmd)
	useFlagFamily(createTableCmd)
	useFlagFields(createTableCmd)
	useFlagKeyFields(createTableCmd)
}

// createTableCmd represents the create-table command
var createTableCmd = &cobra.Command{
	Use:   "create-table [tableName]",
	Short: "Create a new table",
	Long: unindent(`
			Create a new table

			This command makes an HTTP request to the executive service
			to create a new table. 

			Example:

			create-table --family foo --field name:string --field foo:integer --key-field name testtable

			Resulting schema:

			CREATE TABLE foo___testtable (name VARCHAR(191), foo INTEGER, PRIMARY KEY(name));
	`),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
		if err != nil {
			return err
		}
		familyName, err := getFamilyName(cmd)
		if err != nil {
			return err
		}
		fields, err := getFields(cmd)
		if err != nil {
			return err
		}
		keyFields, err := getKeyFields(cmd)
		if err != nil {
			return err
		}
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
