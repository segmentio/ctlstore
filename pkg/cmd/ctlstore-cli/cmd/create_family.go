package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(createFamilyCmd)
	useFlagExecutive(createFamilyCmd)
}

// createFamilyCmd represents the create-family command
var createFamilyCmd = &cobra.Command{
	Use:   "create-family [familyName]",
	Short: "Create a new table family",
	Long: unindent(`
			Create a new table family

			This command makes an HTTP request to the executive service
			to create a new table family. 
	`),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		executive, err := getExecutive(cmd)
		if err != nil {
			return err
		}
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
