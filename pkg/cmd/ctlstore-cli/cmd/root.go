package cmd

import (
	"context"
	"time"

	"github.com/segmentio/cli"
)

func Execute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli.ExecContext(ctx, cli.CommandSet{
		"table-limits":  cliTableLimits,
		"create-table":  cliCreateTable,
		"create-family": cliCreateFamily,
		"add-fields":    cliAddFields,
		"read-keys":     cliReadKeys,
		"read-seq":      cliReadSeq,
		"writer-limits": cliWriterLimits,
	})
}
