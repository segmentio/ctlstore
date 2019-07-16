package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/segmentio/ctlstore"
)

type MyTableRow struct {
	Key string `ctlstore:"mykey"`
	Val string `ctlstore:"myval"`
}

func main() {
	ctx := context.TODO()
	doneCh := make(chan struct{})
	ldbReader, err := ctlstore.Reader()
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		os.Exit(1)
	}

	go func() {
		defer close(doneCh)

		var lastVal MyTableRow
		var foundState bool
		var lastErr error

		for {
			var val MyTableRow
			found, err := ldbReader.GetRowByKey(ctx, &val, "myfamily", "mytable", "hello")
			if err != nil {
				if lastErr != err {
					fmt.Printf("New error reading LDB: %v\n", err)
				}
			} else {
				if found {
					if !reflect.DeepEqual(val, lastVal) {
						fmt.Printf("Got new data for key: %v\n", val)
					}
					foundState = false
				}

				if !found && !foundState {
					fmt.Println("Didn't find key, will let you know when I do!")
					foundState = true
				}

				lastVal = val
			}
			lastErr = err
			time.Sleep(1 * time.Second)
		}
	}()

	<-doneCh
}
