package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
)

type DependencyGraph struct {
	mu    sync.RWMutex
	Nodes map[string]string   // ID -> Status
	Edges map[string][]string // Dependency relations
}

func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		Nodes: make(map[string]string),
		Edges: make(map[string][]string),
	}
}

type WorkloadSpec struct {
	ID                  string            `json:"workload_id"`
	Name                string            `json:"name"`
	Image               string            `json:"image"`
	Ports               []string          `json:"ports,omitempty"`
	Env                 []string          `json:"env,omitempty"`
	CPUShares           int64             `json:"cpu_shares,omitempty"`
	MemoryLimitBytes    int64             `json:"memory_limit_bytes,omitempty"`
	PlacementConstraint string            `json:"placement_constraint,omitempty"`
	MinTrustScore       float64           `json:"min_trust_score,omitempty"`
	DependsOn           []string          `json:"depends_on,omitempty"`
}

// DeployWorkload instantiates the workload container locally under RIB safety policies
func (d *Daemon) DeployWorkload(ctx context.Context, spec WorkloadSpec) (string, error) {
	containerName := "kalpana-" + spec.Name

	// 1. Pull OCI Image
	pullOut, err := d.dockerCli.ImagePull(ctx, spec.Image, dockerimage.PullOptions{})
	if err != nil {
		return "", fmt.Errorf("pull image failed: %w", err)
	}
	io.Copy(io.Discard, pullOut)
	pullOut.Close()

	// 2. Prepare Port Bindings
	portSet := nat.PortSet{}
	portBindings := nat.PortMap{}
	for _, p := range spec.Ports {
		parts := strings.SplitN(p, ":", 2)
		if len(parts) == 2 {
			containerPort, err := nat.NewPort("tcp", parts[1])
			if err != nil {
				continue
			}
			portSet[containerPort] = struct{}{}
			portBindings[containerPort] = []nat.PortBinding{
				{HostIP: "0.0.0.0", HostPort: parts[0]},
			}
		}
	}

	// 3. Prepare config mapping
	config := &container.Config{
		Image:        spec.Image,
		Env:          spec.Env,
		ExposedPorts: portSet,
		Labels: map[string]string{
			"kalpana.managed": "true",
			"kalpana.sirl":    "true",
			"kalpana.service": spec.Name,
		},
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		RestartPolicy: container.RestartPolicy{
			Name: container.RestartPolicyMode("unless-stopped"),
		},
		NetworkMode: container.NetworkMode(d.cfg.Network),
	}
	if spec.MemoryLimitBytes > 0 {
		hostConfig.Memory = spec.MemoryLimitBytes
	}
	if spec.CPUShares > 0 {
		hostConfig.CPUShares = spec.CPUShares
	}

	networkConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			d.cfg.Network: {},
		},
	}

	// 4. Intercept and Sanitize container parameters with RIB
	if err := d.rib.ValidateAndSanitize(config, hostConfig, networkConfig); err != nil {
		// Log security policy violations in governance.db
		_, _ = d.storage.GovernanceDB.Exec(`
			INSERT INTO audit_logs (id, operator, action, resource, outcome)
			VALUES (?, 'RIB', 'deploy', ?, 'denied')
		`, fmt.Sprintf("audit-%d", time.Now().UnixNano()), spec.Name)
		return "", fmt.Errorf("RIB safety check blocked execution: %w", err)
	}

	// Log allowed action in governance.db
	_, _ = d.storage.GovernanceDB.Exec(`
		INSERT INTO audit_logs (id, operator, action, resource, outcome)
		VALUES (?, 'RIB', 'deploy', ?, 'allowed')
	`, fmt.Sprintf("audit-%d", time.Now().UnixNano()), spec.Name)

	// 5. Create and start container
	createResp, err := d.dockerCli.ContainerCreate(ctx,
		config,
		hostConfig,
		networkConfig,
		nil,
		containerName,
	)
	if err != nil {
		return "", fmt.Errorf("container create failed: %w", err)
	}

	if err := d.dockerCli.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		d.dockerCli.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("container start failed: %w", err)
	}

	// 6. Persist to segmented runtime.db
	_, err = d.storage.RuntimeDB.Exec(`
		INSERT OR REPLACE INTO workloads (id, name, image, status, assigned_node, cpu_shares, memory_limit, updated_at)
		VALUES (?, ?, ?, 'running', ?, ?, ?, CURRENT_TIMESTAMP)
	`, spec.ID, spec.Name, spec.Image, d.cfg.NodeID, spec.CPUShares, spec.MemoryLimitBytes)
	if err != nil {
		log.Printf("[SRK] SQLite save error: %v", err)
	}

	// 7. Update Dependency Graph
	d.graph.mu.Lock()
	d.graph.Nodes[spec.ID] = "running"
	if len(spec.DependsOn) > 0 {
		d.graph.Edges[spec.ID] = spec.DependsOn
	}
	d.graph.mu.Unlock()

	log.Printf("[SRK] Successfully deployed unprivileged container %s (ID: %s)", spec.Name, createResp.ID[:12])
	return createResp.ID[:12], nil
}

// TerminateWorkload stops and removes the workload container
func (d *Daemon) TerminateWorkload(ctx context.Context, workloadID string) error {
	var name string
	err := d.storage.RuntimeDB.QueryRow("SELECT name FROM workloads WHERE id = ?", workloadID).Scan(&name)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("workload not found in database: %s", workloadID)
		}
		return err
	}

	containerName := "kalpana-" + name
	containers, err := d.dockerCli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return err
	}

	var cID string
	for _, c := range containers {
		for _, n := range c.Names {
			if strings.TrimPrefix(n, "/") == containerName {
				cID = c.ID
				break
			}
		}
	}

	if cID != "" {
		timeout := 10
		stopOpts := container.StopOptions{Timeout: &timeout}
		_ = d.dockerCli.ContainerStop(ctx, cID, stopOpts)
		if err := d.dockerCli.ContainerRemove(ctx, cID, container.RemoveOptions{Force: true}); err != nil {
			return fmt.Errorf("remove container failed: %w", err)
		}
	}

	_, err = d.storage.RuntimeDB.Exec("UPDATE workloads SET status = 'terminated', updated_at = CURRENT_TIMESTAMP WHERE id = ?", workloadID)
	if err != nil {
		log.Printf("[SRK] DB update error: %v", err)
	}

	d.graph.mu.Lock()
	d.graph.Nodes[workloadID] = "terminated"
	d.graph.mu.Unlock()

	log.Printf("[SRK] Successfully terminated container %s", name)
	return nil
}
