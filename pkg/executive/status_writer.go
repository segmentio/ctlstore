package executive

import "net/http"

type statusWriter struct {
	writer http.ResponseWriter
	code   int
}

func (w *statusWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *statusWriter) Write(b []byte) (int, error) {
	return w.writer.Write(b)
}

func (w *statusWriter) WriteHeader(statusCode int) {
	w.code = statusCode
	w.writer.WriteHeader(statusCode)
}
