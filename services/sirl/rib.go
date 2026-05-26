package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
)

type RuntimeIsolationBroker struct {
	cfg Config
}

func NewRuntimeIsolationBroker(cfg Config) *RuntimeIsolationBroker {
	return &RuntimeIsolationBroker{cfg: cfg}
}

// ValidateAndSanitize inspects and alters container configurations to enforce edge-node unprivileged isolation policies
func (rib *RuntimeIsolationBroker) ValidateAndSanitize(config *container.Config, hostConfig *container.HostConfig, networkConfig *network.NetworkingConfig) error {
	if config == nil || hostConfig == nil {
		return fmt.Errorf("invalid container configurations")
	}

	// 1. Block privileged containers
	if hostConfig.Privileged {
		return fmt.Errorf("security violation: privileged containers are blocked by the Runtime Isolation Broker")
	}

	// 2. Block host networking
	if hostConfig.NetworkMode.IsHost() {
		return fmt.Errorf("security violation: host networking is blocked by the Runtime Isolation Broker")
	}

	// 3. Enforce capability filtering
	// Drop ALL standard capabilities by default
	hostConfig.CapDrop = []string{"ALL"}
	// Permit only NET_BIND_SERVICE to allow binding to low ports (<1024) if strictly necessary
	hostConfig.CapAdd = []string{"NET_BIND_SERVICE"}

	// 4. Prevent privilege escalation
	hasNoNewPrivs := false
	for _, opt := range hostConfig.SecurityOpt {
		if strings.Contains(opt, "no-new-privileges") {
			hasNoNewPrivs = true
			break
		}
	}
	if !hasNoNewPrivs {
		hostConfig.SecurityOpt = append(hostConfig.SecurityOpt, "no-new-privileges:true")
	}

	// 5. Enforce seccomp filters (ensure we don't bypass them)
	for _, opt := range hostConfig.SecurityOpt {
		if strings.Contains(opt, "seccomp=unconfined") {
			return fmt.Errorf("security violation: seccomp bypass is blocked by the Runtime Isolation Broker")
		}
	}

	// 6. Validate mounts (prevent root or system directories escapes)
	for _, b := range hostConfig.Binds {
		parts := strings.Split(b, ":")
		if len(parts) > 0 {
			hostPath := parts[0]
			if err := rib.checkHostPathSafety(hostPath); err != nil {
				return fmt.Errorf("mount safety violation on %s: %w", hostPath, err)
			}
		}
	}

	for _, m := range hostConfig.Mounts {
		if err := rib.checkHostPathSafety(m.Source); err != nil {
			return fmt.Errorf("mount safety violation on %s: %w", m.Source, err)
		}
	}

	return nil
}

func (rib *RuntimeIsolationBroker) checkHostPathSafety(hostPath string) error {
	cleanPath := filepath.Clean(hostPath)

	// Block direct mappings to root or dangerous systems directories
	forbiddenPrefixes := []string{
		"/", "/etc", "/sys", "/var", "/boot", "/dev", "/root", "/home", "/bin", "/sbin", "/lib",
	}

	for _, p := range forbiddenPrefixes {
		if cleanPath == p {
			return fmt.Errorf("direct mount of system root/directory %s is blocked", p)
		}
	}

	// Traversal safety: must remain within the /data boundary or kalpana directories
	if !strings.HasPrefix(cleanPath, "/data") && !strings.Contains(cleanPath, "kalpana") {
		return fmt.Errorf("bind mounts must reside within unprivileged workspace boundaries (/data)")
	}

	if strings.Contains(cleanPath, "..") {
		return fmt.Errorf("relative path traversal is blocked")
	}

	return nil
}
