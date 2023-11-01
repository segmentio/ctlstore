package logwriter

import (
	"errors"
	"os"
	"strings"
)

const sizedLogWriterDefaultMode os.FileMode = 0644

// Implements a line-by-line log file writer that appends to a file
// specified by Path until it reaches RotateSize bytes, at which point
// it will delete the file and start over with a fresh one.
//
// Make sure to call Close() after this is no longer needed.
type SizedLogWriter struct {
	RotateSize int
	Path       string
	FileMode   os.FileMode

	_f *os.File // don't use this directly, use file()
}

func (w *SizedLogWriter) Mode() os.FileMode {
	if w.FileMode == 0 {
		return sizedLogWriterDefaultMode
	}

	return w.FileMode
}

func (w *SizedLogWriter) File() (*os.File, error) {
	if w._f != nil {
		return w._f, nil
	}

	f, err := os.OpenFile(w.Path, os.O_CREATE|os.O_RDWR, w.Mode())
	if err != nil {
		return nil, err
	}

	w._f = f
	return w._f, nil
}

func (w *SizedLogWriter) Rotate() error {
	var err error

	if w._f != nil {
		err = w._f.Close()
		if err != nil {
			return err
		}
		w._f = nil
	}

	err = os.Remove(w.Path)
	if err != nil {
		return err
	}

	return nil
}

// Close cleans up the associated resources
func (w *SizedLogWriter) Close() error {
	if w._f != nil {
		err := w._f.Close()
		w._f = nil
		return err
	}
	return nil
}

// WriteLine appends a line to the end of the log file. If the log line would
// exceed the set RotateSize, then the log file will be rotated, and the line
// will be appended to the new log file.
func (w *SizedLogWriter) WriteLine(line string) error {
	f, err := w.File()
	if err != nil {
		return err
	}

	if strings.ContainsRune(line, '\n') {
		return errors.New("Lines can't contain a carriage-return")
	}

	if len(line) > w.RotateSize {
		return errors.New("Line length is > RotateSize")
	}

	offset, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	newEndOffset := offset + int64(len(line))
	if newEndOffset > int64(w.RotateSize) {
		err = w.Rotate()
		if err != nil {
			return err
		}

		f, err = w.File()
		if err != nil {
			return err
		}
	}

	bytes := []byte(line)
	bytes = append(bytes, byte('\n'))
	_, err = f.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
