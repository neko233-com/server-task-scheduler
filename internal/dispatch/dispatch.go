// Package dispatch sends execution requests to worker nodes and parses their
// responses. It is the server->worker half of the protocol.
package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/neko233-com/server-task-scheduler/internal/model"
)

// Dispatcher performs HTTP calls to workers.
type Dispatcher struct {
	client *http.Client
}

// New returns a Dispatcher with sane defaults.
func New() *Dispatcher {
	return &Dispatcher{
		client: &http.Client{Timeout: 0}, // per-request timeout is applied via context
	}
}

// Execute calls {worker.Address}/execute and returns the worker's response.
// The call is bounded by req.TimeoutSeconds (with a small network overhead).
func (d *Dispatcher) Execute(ctx context.Context, worker *model.Worker, req model.ExecuteRequest) (model.ExecuteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return model.ExecuteResponse{}, err
	}
	url := fmt.Sprintf("%s/execute", worker.Address)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return model.ExecuteResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	timeout += 5 * time.Second // network slack
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	httpReq = httpReq.WithContext(cctx)

	resp, err := d.client.Do(httpReq)
	if err != nil {
		return model.ExecuteResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return model.ExecuteResponse{}, fmt.Errorf("worker returned status %d", resp.StatusCode)
	}
	var ex model.ExecuteResponse
	if err := json.NewDecoder(resp.Body).Decode(&ex); err != nil {
		return model.ExecuteResponse{}, err
	}
	return ex, nil
}
