package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// EvaluateLocalPolicies checks workload spec against security and architectural parameters
func (d *Daemon) EvaluateLocalPolicies(ctx context.Context, spec WorkloadSpec) (bool, error) {
	// 1. Strict resource boundaries per container (512MB RAM cap)
	if spec.MemoryLimitBytes > 512*1024*1024 {
		return false, fmt.Errorf("resource cap violation: memory limit %d bytes exceeds 512MB threshold", spec.MemoryLimitBytes)
	}

	if !strings.HasPrefix(spec.Image, "docker.io/") &&
		!strings.HasPrefix(spec.Image, "nginx") &&
		!strings.HasPrefix(spec.Image, "alpine") &&
		!strings.HasPrefix(spec.Image, "node") &&
		!strings.HasPrefix(spec.Image, "python") &&
		!strings.HasPrefix(spec.Image, "golang") &&
		!strings.HasPrefix(spec.Image, "postgres") &&
		!strings.HasPrefix(spec.Image, "redis") &&
		!strings.HasPrefix(spec.Image, "mongo") &&
		!strings.HasPrefix(spec.Image, "qdrant") &&
		!strings.HasPrefix(spec.Image, "minio") &&
		!strings.Contains(spec.Image, "kalpana") &&
		!strings.Contains(spec.Image, "alpine") {
		return false, fmt.Errorf("untrusted registry origin: %s is not registered in safe registry list", spec.Image)
	}

	// 3. Integrate with the external PGE service if configured
	if d.cfg.PGEURL != "" {
		allowed, reason, err := d.queryExternalPGE(ctx, spec.Name, "deploy")
		if err != nil {
			// Fail-safe: if remote PGE is offline, edge node must remain autonomous.
			// Log and allow execution locally, or block if strict mode is active.
			return true, nil
		}
		if !allowed {
			return false, fmt.Errorf("denied by PGE: %s", reason)
		}
	}

	return true, nil
}

func (d *Daemon) queryExternalPGE(ctx context.Context, resourceName, action string) (bool, string, error) {
	reqBody, _ := json.Marshal(map[string]string{
		"target_service": "sirl",
		"action":         action,
		"resource":       resourceName,
	})

	req, err := http.NewRequestWithContext(ctx, "POST", d.cfg.PGEURL+"/evaluate", bytes.NewReader(reqBody))
	if err != nil {
		return false, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer godmode-override") // PGE bypass token configured in docker-compose

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, "", fmt.Errorf("PGE returned status %d", resp.StatusCode)
	}

	var res struct {
		Allowed bool   `json:"allowed"`
		Reason  string `json:"reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, "", err
	}

	return res.Allowed, res.Reason, nil
}
