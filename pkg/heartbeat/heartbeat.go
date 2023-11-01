package heartbeat

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/events/v2"
)

type (
	Heartbeat struct {
		interval     time.Duration
		executive    string
		writerName   string
		writerSecret string
		family       string
		table        string
	}
	HeartbeatConfig struct {
		HeartbeatInterval time.Duration
		ExecutiveURL      string
		Family            string
		Table             string
		WriterName        string
		WriterSecret      string
	}
)

var (
	client = &http.Client{}
)

func HeartbeatFromConfig(config HeartbeatConfig) (*Heartbeat, error) {
	url := config.ExecutiveURL
	if !strings.HasPrefix(url, "http") {
		url = "http://" + url
	}
	heartbeat := &Heartbeat{
		family:       config.Family,
		table:        config.Table,
		interval:     config.HeartbeatInterval,
		executive:    url,
		writerName:   config.WriterName,
		writerSecret: config.WriterSecret,
	}
	if err := heartbeat.init(); err != nil {
		return nil, fmt.Errorf("init heartbeat: %w", err)
	}
	return heartbeat, nil
}

func (h *Heartbeat) Start(ctx context.Context) {
	events.Log("Heartbeat starting")
	defer events.Log("Heartbeat stopped")
	utils.CtxFireLoop(ctx, h.interval, func() { h.pulse(ctx) })
}

func (h *Heartbeat) pulse(ctx context.Context) {
	err := func() error {
		type mutation struct {
			Table  string                 `json:"table"`
			Delete bool                   `json:"delete"`
			Values map[string]interface{} `json:"values"`
		}
		type payload struct {
			Cookie    []byte     `json:"cookie"`
			Mutations []mutation `json:"mutations"`
		}
		heartbeat := time.Now().UnixNano()
		body := utils.NewJsonReader(payload{
			Mutations: []mutation{
				{
					Table:  h.table,
					Delete: false,
					Values: map[string]interface{}{
						"name":  "heartbeat",
						"value": heartbeat,
					},
				},
			},
		})
		req, err := http.NewRequest(http.MethodPost, h.executive+"/families/"+h.family+"/mutations", body)
		if err != nil {
			return fmt.Errorf("build mutation request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("ctlstore-writer", h.writerName)
		req.Header.Set("ctlstore-secret", h.writerSecret)
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("make mutation request: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("could not make mutation request: %d: %s", resp.StatusCode, b)
		}
		events.Log("Heartbeat: %v", heartbeat)
		return nil
	}()
	if err != nil {
		events.Log("Heartbeat failed: %s", err)
		errs.Incr("heartbeat-errors")
	}
}

func (h *Heartbeat) init() error {

	// register the writer ----------

	body := strings.NewReader(h.writerSecret)
	res, err := http.Post(h.executive+"/writers/"+h.writerName, "text/plain", body)
	if err != nil {
		return fmt.Errorf("register writer: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("could not register writer: %d: %s", res.StatusCode, b)
	}

	// setup the family ------------

	req, err := http.NewRequest(http.MethodPost, h.executive+"/families/"+h.family, nil)
	if err != nil {
		return fmt.Errorf("create family request: %w", err)
	}
	res, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("make family request: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("could not make family request: %v: %s", res.StatusCode, b)
	}

	// setup the table -------------

	var tableDef = struct {
		Fields    [][]string `json:"fields"`
		KeyFields []string   `json:"keyFields"`
	}{
		Fields:    [][]string{{"name", "string"}, {"value", "integer"}},
		KeyFields: []string{"name"},
	}
	req, err = http.NewRequest(http.MethodPost, h.executive+"/families/"+h.family+"/tables/"+h.table, utils.NewJsonReader(tableDef))
	if err != nil {
		return fmt.Errorf("create table request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("make table request: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("could not make table request: %v: %s", res.StatusCode, b)
	}

	return nil
}

func (h *Heartbeat) Close() error {
	return nil
}
