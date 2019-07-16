package supervisor

import (
	"fmt"
	"io"

	"github.com/segmentio/ctlstore/pkg/utils"
)

type fakeReadCloser struct {
	rc          io.ReadCloser
	readErr     error
	readCalled  utils.AtomicBool
	closeErr    error
	closeCalled utils.AtomicBool
}

func (r *fakeReadCloser) Read(p []byte) (n int, err error) {
	r.readCalled.SetTrue()
	n, err = r.rc.Read(p)
	if r.readErr != nil {
		err = r.readErr
	}
	return n, err
}

func (r *fakeReadCloser) Close() error {
	r.closeCalled.SetTrue()
	err := r.rc.Close()
	if r.closeErr != nil {
		fmt.Println("Returning", r.closeErr)
		err = r.closeErr
	}
	return err
}
