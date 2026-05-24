package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type AnomalyItem struct {
	ID               string   `json:"id"`
	Severity         string   `json:"severity"`
	Title            string   `json:"title"`
	Description      string   `json:"description"`
	ServicesAffected []string `json:"services_affected"`
	DetectedAt       string   `json:"detected_at"`
	Resolved         bool     `json:"resolved"`
}

func (s *Server) runRemediationAgent(ctx context.Context, input string) (string, error) {
	tokenStr, _ := ctx.Value(contextKeyToken).(string)
	if tokenStr == "" {
		return "", fmt.Errorf("missing credentials in context")
	}

	log.Printf("[RemediationAgent] running self-healing diagnostic scan...")

	// 1. Fetch unresolved anomalies from AICP
	req, err := http.NewRequestWithContext(ctx, "GET", s.cfg.AICPURL+"/anomalies?resolved=false", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tokenStr)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to query AICP anomalies: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("AICP /anomalies returned %d: %s", resp.StatusCode, string(body))
	}

	var rawAnomalies struct {
		Anomalies []AnomalyItem `json:"anomalies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rawAnomalies); err != nil {
		return "", fmt.Errorf("failed to decode anomalies: %w", err)
	}

	unresolved := rawAnomalies.Anomalies
	if len(unresolved) == 0 {
		return "No unresolved anomalies detected. Infrastructure health is optimal.", nil
	}

	var report strings.Builder
	report.WriteString("# Autonomous Self-Healing Report\n")
	report.WriteString(fmt.Sprintf("**Executed At:** %s\n\n", time.Now().Format(time.RFC3339)))

	remediatedCount := 0

	for _, anomaly := range unresolved {
		// Filter by severity (HIGH or CRITICAL)
		sev := strings.ToUpper(anomaly.Severity)
		if sev != "HIGH" && sev != "CRITICAL" {
			log.Printf("[RemediationAgent] skipping low severity anomaly %s (%s)", anomaly.ID, anomaly.Severity)
			continue
		}

		report.WriteString(fmt.Sprintf("## Remediation Action for Anomaly: [%s] %s\n", anomaly.Severity, anomaly.Title))
		report.WriteString(fmt.Sprintf("- **Description:** %s\n", anomaly.Description))

		// Determine remediation action
		action := "restart" // default
		descLower := strings.ToLower(anomaly.Description + " " + anomaly.Title)
		if strings.Contains(descLower, "oom") || strings.Contains(descLower, "out of memory") || strings.Contains(descLower, "crash") || strings.Contains(descLower, "deadlock") || strings.Contains(descLower, "timeout") {
			action = "restart"
		} else if strings.Contains(descLower, "high load") || strings.Contains(descLower, "traffic") || strings.Contains(descLower, "cpu") || strings.Contains(descLower, "scale") {
			action = "scale"
		} else if strings.Contains(descLower, "rollback") || strings.Contains(descLower, "regression") || strings.Contains(descLower, "broken") {
			action = "rollback"
		}

		services := anomaly.ServicesAffected
		if len(services) == 0 {
			// Try to parse from description
			for _, svc := range []string{"sil", "col", "ssi", "aicp", "aaf", "orchestrator", "qdrant", "nats"} {
				if strings.Contains(descLower, svc) {
					services = append(services, svc)
				}
			}
		}

		if len(services) == 0 {
			report.WriteString("- **Result:** Failed. No affected services identified in anomaly.\n\n")
			continue
		}

		report.WriteString(fmt.Sprintf("- **Target Services:** %s\n", strings.Join(services, ", ")))
		report.WriteString(fmt.Sprintf("- **Remediation Action:** %s\n", action))

		success := true
		var actionErrs []string

		for _, service := range services {
			err := s.executeRecoveryAction(ctx, action, service, tokenStr)
			if err != nil {
				success = false
				actionErrs = append(actionErrs, fmt.Sprintf("%s: %v", service, err))
			} else {
				// Secure log to Vault
				s.logToVaultBestEffort(ctx, action, service, fmt.Sprintf("Anomaly: %s. Resolved: true.", anomaly.Title))
			}
		}

		if success {
			// Resolve anomaly in AICP
			resolveErr := s.resolveAnomalyInAICP(ctx, anomaly.ID, fmt.Sprintf("RemediationAgent auto-healed via %s", action), tokenStr)
			if resolveErr != nil {
				report.WriteString(fmt.Sprintf("- **Result:** Actions succeeded, but failed to resolve anomaly record in AICP: %v\n\n", resolveErr))
			} else {
				report.WriteString(fmt.Sprintf("- **Result:** Success. Anomaly marked as resolved.\n\n"))
				remediatedCount++
			}
		} else {
			report.WriteString(fmt.Sprintf("- **Result:** Failed. Errors: %s\n\n", strings.Join(actionErrs, "; ")))
		}
	}

	report.WriteString(fmt.Sprintf("### Summary\n- Total anomalies scanned: %d\n- Successfully remediated: %d\n", len(unresolved), remediatedCount))
	return report.String(), nil
}

func (s *Server) executeRecoveryAction(ctx context.Context, action, service, tokenStr string) error {
	var url string
	var body []byte

	switch action {
	case "restart":
		url = fmt.Sprintf("%s/services/%s/restart", s.cfg.COLURL, service)
	case "scale":
		url = fmt.Sprintf("%s/services/%s/scale", s.cfg.COLURL, service)
		body, _ = json.Marshal(map[string]int{"replicas": 3})
	case "rollback":
		url = fmt.Sprintf("%s/services/%s/rollback", s.cfg.COLURL, service)
	default:
		return fmt.Errorf("unknown action %q", action)
	}

	var req *http.Request
	var err error
	if len(body) > 0 {
		req, err = http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	} else {
		req, err = http.NewRequestWithContext(ctx, "POST", url, nil)
	}

	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+tokenStr)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("COL returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (s *Server) resolveAnomalyInAICP(ctx context.Context, anomalyID, note, tokenStr string) error {
	url := fmt.Sprintf("%s/anomalies/%s/resolve", s.cfg.AICPURL, anomalyID)
	body, _ := json.Marshal(map[string]string{"note": note})

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+tokenStr)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("AICP returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (s *Server) logToVaultBestEffort(ctx context.Context, action, service, detail string) {
	vaultURL := "http://vault:8200/v1/secret/data/remediation/" + service
	payload := map[string]interface{}{
		"data": map[string]interface{}{
			"action":    action,
			"service":   service,
			"detail":    detail,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		},
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", vaultURL, bytes.NewBuffer(body))
	if err != nil {
		return
	}

	token := os.Getenv("VAULT_TOKEN")
	if token == "" {
		token = "root"
	}
	req.Header.Set("X-Vault-Token", token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 1 * time.Second}
	resp, err := client.Do(req)
	if err == nil {
		resp.Body.Close()
	} else {
		log.Printf("[RemediationAgent] best-effort Vault logging failed: %v", err)
	}
}
