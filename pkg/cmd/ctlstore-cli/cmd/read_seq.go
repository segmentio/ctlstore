package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/cli"
	"github.com/segmentio/ctlstore"
)

var cliReadSeq = &cli.CommandFunc{
	Help: "Read last sequenece from the LDB",
	Func: func(ctx context.Context, config struct {
		flagLDBPath
	}) error {
		ldbPath := config.LDBPath
		reader, err := ctlstore.ReaderForPath(ldbPath)
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
