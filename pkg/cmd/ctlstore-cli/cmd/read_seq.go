package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/ctlstore"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(readSeqCmd)
	useFlagLDB(readSeqCmd)
}

// readSeqCmd represents the read-seq command
var readSeqCmd = &cobra.Command{
	Use:   "read-seq",
	Short: "Read last sequence from the ldb",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ldb, err := getLDB(cmd)
		if err != nil {
			return err
		}
		reader, err := ctlstore.ReaderForPath(ldb)
		if err != nil {
			bail("Could not get reader: %s", err)
		}
		defer reader.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		seq, err := reader.GetLastSequence(ctx)
		if err != nil {
			bail("Could not get sequence: %s", err)
		}
		fmt.Println(seq)
		return nil
	},
}
