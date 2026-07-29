package node

import (
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestMetricsExposeOperationalCounters(t *testing.T) {
	var metrics nodeMetrics
	metrics.rpcTotal.Add(3)
	response := httptest.NewRecorder()
	metrics.handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body := response.Body.String()
	if !strings.Contains(body, "quorumkv_rpc_total 3") {
		t.Fatalf("metrics output missing observed counter value:\n%s", body)
	}

	// Every published name must be written by production code. Asserting the
	// exact set makes an unwritten, permanently zero metric a deliberate change
	// rather than an oversight.
	published := make([]string, 0)
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		name, _, found := strings.Cut(strings.TrimSpace(line), " ")
		if !found {
			t.Fatalf("metrics line %q is not a name/value pair", line)
		}
		published = append(published, name)
	}
	want := []string{
		"quorumkv_rpc_total",
		"quorumkv_rpc_errors_total",
		"quorumkv_elections_total",
		"quorumkv_raft_rpcs_total",
		"quorumkv_proposals_total",
		"quorumkv_client_errors_total",
		"quorumkv_wal_syncs_total",
		"quorumkv_snapshots_total",
		"quorumkv_snapshot_installations_total",
		"quorumkv_snapshot_compactions_total",
	}
	if !slices.Equal(published, want) {
		t.Fatalf("published metrics = %v, want %v", published, want)
	}
}
