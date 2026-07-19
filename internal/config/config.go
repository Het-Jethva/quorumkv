package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config contains the static identity and local settings for one node.
type Config struct {
	Version   int    `yaml:"version"`
	ClusterID string `yaml:"cluster_id"`
	Node      Node   `yaml:"node"`
}

// Node contains settings owned by this process.
type Node struct {
	ID            string `yaml:"id"`
	PeerAddress   string `yaml:"peer_address"`
	ClientAddress string `yaml:"client_address"`
	DataDir       string `yaml:"data_dir"`
}

// Load reads and validates a node configuration file.
func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	decoder := yaml.NewDecoder(strings.NewReader(string(contents)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}

	if !filepath.IsAbs(cfg.Node.DataDir) {
		cfg.Node.DataDir = filepath.Clean(filepath.Join(filepath.Dir(path), cfg.Node.DataDir))
	}
	return cfg, nil
}

// Validate rejects ambiguous or incomplete local settings before startup.
func (c Config) Validate() error {
	var problems []error
	if c.Version != 1 {
		problems = append(problems, fmt.Errorf("version must be 1, got %d", c.Version))
	}
	if strings.TrimSpace(c.ClusterID) == "" {
		problems = append(problems, errors.New("cluster_id is required"))
	}
	if strings.TrimSpace(c.Node.ID) == "" {
		problems = append(problems, errors.New("node.id is required"))
	}
	if err := validateAddress("node.peer_address", c.Node.PeerAddress); err != nil {
		problems = append(problems, err)
	}
	if err := validateAddress("node.client_address", c.Node.ClientAddress); err != nil {
		problems = append(problems, err)
	}
	if c.Node.PeerAddress != "" && c.Node.PeerAddress == c.Node.ClientAddress {
		problems = append(problems, errors.New("node peer and client addresses must be different"))
	}
	if strings.TrimSpace(c.Node.DataDir) == "" {
		problems = append(problems, errors.New("node.data_dir is required"))
	}
	return errors.Join(problems...)
}

func validateAddress(name, address string) error {
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("%s is required", name)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%s must be a host:port address: %w", name, err)
	}
	if host == "" {
		return fmt.Errorf("%s host is required", name)
	}
	if port == "" || port == "0" {
		return fmt.Errorf("%s port must be non-zero", name)
	}
	return nil
}
