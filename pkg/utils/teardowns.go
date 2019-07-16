package utils

// Teardowns is meant to make it easy to chain teardown funcs and
// then have them execute in reverse order (like defer)
type Teardowns struct {
	funcs []func()
}

func (t *Teardowns) Add(fn func()) {
	t.funcs = append(t.funcs, fn)
}

func (t *Teardowns) Teardown() {
	for i := len(t.funcs) - 1; i >= 0; i-- {
		t.funcs[i]()
	}
}
