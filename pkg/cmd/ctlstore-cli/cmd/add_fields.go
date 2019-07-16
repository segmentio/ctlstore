package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(addFieldsCmd)
	useFlagExecutive(addFieldsCmd)
	useFlagFamily(addFieldsCmd)
	useFlagTable(addFieldsCmd)
	useFlagFields(addFieldsCmd)
}

// addFieldsCmd represents the add-fields command
var addFieldsCmd = &cobra.Command{
	Use:   "add-fields",
	Short: "Add fields to an existing table",
	Long: unindent(`
			Add fields to an existing table

			This command makes an HTTP request to the executive service
			to add fields to an existing table

			Example:

			add-fields --family foo --table bar --field name:string
	`),
	Args: cobra.NoArgs,
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
		fields, err := getFields(cmd)
		if err != nil {
			return err
		}

		// todo: dedupe this declaration
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
