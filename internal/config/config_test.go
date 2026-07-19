package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/config"
)

func TestLoadValidConfig(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "node.yaml")
	contents := `version: 1
cluster_id: test-cluster
node:
  id: node-1
  peer_address: 127.0.0.1:7300
  client_address: 127.0.0.1:7400
  data_dir: data/node-1
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ClusterID != "test-cluster" || cfg.Node.ID != "node-1" {
		t.Fatalf("Load() identity = %q/%q, want test-cluster/node-1", cfg.ClusterID, cfg.Node.ID)
	}
	wantDataDir := filepath.Join(directory, "data", "node-1")
	if cfg.Node.DataDir != wantDataDir {
		t.Fatalf("Load() data dir = %q, want %q", cfg.Node.DataDir, wantDataDir)
	}
}

func TestLoadRejectsUnknownAndInvalidSettings(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "node.yaml")
	contents := `version: 2
cluster_id: ""
unexpected: true
node:
  id: ""
  peer_address: 127.0.0.1:7300
  client_address: 127.0.0.1:7300
  data_dir: ""
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want invalid configuration error")
	}
	if !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("Load() error = %q, want unknown-field detail", err)
	}
}

func TestValidateReportsAllInvalidSettings(t *testing.T) {
	t.Parallel()

	err := (config.Config{}).Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want validation errors")
	}
	for _, detail := range []string{"version", "cluster_id", "node.id", "node.peer_address", "node.client_address", "node.data_dir"} {
		if !strings.Contains(err.Error(), detail) {
			t.Errorf("Validate() error = %q, want %q", err, detail)
		}
	}
}
