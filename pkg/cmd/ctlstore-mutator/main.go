// This program sends constant load to the executive service.  It is intended to be used in a test
// or simulation environment.  The first use of it will be to generate load so that output can be
// queried using the ctlstore-cli.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/conf"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/errors-go"
)

type config struct {
	ExecutiveEndpoint string `conf:"executive"`
	WriterName        string `conf:"writer-name"`
	WriterSecret      string `conf:"writer-secret"`
	FamilyName        string `conf:"family-name"`
	TableName         string `conf:"table-name"`
}

const (
	// how much to sleep between mutations
	sleepTime = 1 * time.Second
)

var (
	client = &http.Client{}
)

func main() {
	cfg := config{
		ExecutiveEndpoint: "executive",
		WriterName:        "load-writer",
		WriterSecret:      "load-writer-secret",
		FamilyName:        "loadfamily",
		TableName:         "loadtable",
	}
	conf.Load(&cfg)
	executiveURL := fmt.Sprintf("http://%s", cfg.ExecutiveEndpoint)
	for {
		if err := setup(cfg, executiveURL); err != nil {
			fmt.Println("Setup failed:", err)
			time.Sleep(time.Second)
			continue
		}
		break
	}

	type mutation struct {
		Table  string                 `json:"table"`
		Delete bool                   `json:"delete"`
		Values map[string]interface{} `json:"values"`
	}
	type payload struct {
		Cookie    []byte     `json:"cookie"`
		Mutations []mutation `json:"mutations"`
	}

	// start sending the mutations
	iter := uint64(time.Now().UnixNano())

	for {
		iter += 1
		err := func() error {
			cookie := make([]byte, 8)
			binary.BigEndian.PutUint64(cookie, iter)
			cookieStr := hex.EncodeToString(cookie)
			payload := payload{
				Cookie: cookie,
				Mutations: []mutation{
					{
						Table:  cfg.TableName,
						Delete: false,
						Values: map[string]interface{}{
							"type":  "general",
							"name":  "cookie",
							"value": cookieStr,
						},
					},
					{
						Table:  cfg.TableName,
						Delete: false,
						Values: map[string]interface{}{
							"type":  "upcased",
							"name":  "cookie",
							"value": strings.ToUpper(cookieStr),
						},
					},
				},
			}
			b, err := json.Marshal(payload)
			if err != nil {
				return errors.Wrap(err, "marshaling payload")
			}
			req, err := http.NewRequest("POST", executiveURL+"/families/"+cfg.FamilyName+"/mutations", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("ctlstore-writer", cfg.WriterName)
			req.Header.Set("ctlstore-secret", cfg.WriterSecret)
			resp, err := client.Do(req)
			if err != nil {
				return errors.Wrap(err, "making mutation request")
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				b, _ = ioutil.ReadAll(resp.Body)
				return fmt.Errorf("could not make mutation request: %d: %s", resp.StatusCode, b)
			}
			return nil
		}()
		if err != nil {
			fmt.Println("Mutation failed:", err)
		}
		time.Sleep(sleepTime)
	}

}

// setup does all the prep on the ctldb before it can start sending
// mutations
func setup(cfg config, url string) error {

	// register the writer first

	body := strings.NewReader(cfg.WriterSecret)
	res, err := http.Post(url+"/writers/"+cfg.WriterName, "text/plain", body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("could register writer: %v: %s", res.StatusCode, b)
	}

	// create the family name

	req, err := http.NewRequest("POST", url+"/families/"+cfg.FamilyName, nil)
	if err != nil {
		return errors.Wrap(err, "create family request")
	}
	req.Header.Set("Content-Type", "application/json")
	res, err = client.Do(req)
	if err != nil {
		return errors.Wrap(err, "making faily request")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("could not make family request: %v: %s", res.StatusCode, b)
	}

	fmt.Println("Registered family:", cfg.FamilyName)

	// create the table name

	var tableDef = struct {
		Fields    [][]string `json:"fields"`
		KeyFields []string   `json:"keyFields"`
	}{
		Fields: [][]string{
			{"type", "string"},
			{"name", "string"},
			{"value", "string"},
		},
		KeyFields: []string{"type", "name"},
	}
	req, err = http.NewRequest("POST", url+"/families/"+cfg.FamilyName+"/tables/"+cfg.TableName, utils.NewJsonReader(tableDef))
	if err != nil {
		return errors.Wrap(err, "create family request")
	}
	req.Header.Set("Content-Type", "application/json")
	res, err = client.Do(req)
	if err != nil {
		return errors.Wrap(err, "making table request")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("could not make table request: %v: %s", res.StatusCode, b)
	}

	fmt.Println("Registered table:", cfg.TableName)
	return nil
}
