package scanfunc

// Placeholder for columns which have no corresponding field in the
// target struct.
type NoOpScanner struct{}

func (s *NoOpScanner) Scan(src interface{}) error {
	return nil
}
