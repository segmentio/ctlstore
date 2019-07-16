package executive

import (
	"net/http"
	"strings"
	"testing"
)

func TestTestExecutiveService(t *testing.T) {
	svc, err := NewTestExecutiveService("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	{
		resp, err := http.Get("http://" + svc.Addr.String() + "/status")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if want, got := 200, resp.StatusCode; want != got {
			t.Errorf("Expected status code: %v, got %v", want, got)
		}
	}

	{
		ior := strings.NewReader("")
		resp, err := http.Post("http://"+svc.Addr.String()+"/families/test1", "text/plain", ior)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if want, got := 200, resp.StatusCode; want != got {
			t.Errorf("Expected status code: %v, got %v", want, got)
		}
	}
}
