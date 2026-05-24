# KalpanaOS — Distributed Sovereign Cognitive Infrastructure

**KalpanaOS** is a next-generation, AI-native infrastructure operating system built fundamentally for resource-constrained edge environments. Specifically engineered to operate flawlessly on hosts with a strict **4GB RAM limit**, it abandons traditional, declarative, YAML-heavy DevOps pipelines.

Instead, KalpanaOS introduces a **Distributed Cognitive Infrastructure**: a self-healing, peer-to-peer federated mesh where autonomous AI agents orchestrate workloads, manage semantic memory, govern infrastructure policies, and actively resolve anomalies without human intervention.

---

## 🧠 Core Philosophy & Constraints

1.  **AI-Native Operations**: The control plane isn't a CLI or a dashboard; it's a conversational AI equipped with Retrieval-Augmented Generation (RAG) that actively understands infrastructure topology.
2.  **Sovereignty & Isolation**: Built entirely on decentralized tools (SQLite, NATS, Qdrant, Docker API). Initially built with local LLM support, KalpanaOS now dynamically offloads heavy inference to the NVIDIA AI API to preserve extreme host efficiency.
3.  **Federated Edge Mesh**: Nodes don't report to a single master. They gossip their agent capabilities over NATS and intelligently route workloads to the most capable peer.
4.  **Memory Lifecycle Safety**: Infrastructure systems generate massive data. KalpanaOS uses AI-driven compression to perpetually synthesize raw logs into dense semantic insights, guaranteeing the host never exhausts disk or RAM.

---

## 🏗️ Architectural Deep Dive

KalpanaOS is structured as a decentralized suite of lightweight Go microservices connected via a resilient NATS JetStream event bus and private Docker networks. The ecosystem was built iteratively across **Five Core Phases**:

### Phase 1: The Foundation (Identity & Execution)
*   **SIL (Sovereign Identity Layer):** A zero-trust RBAC service managing JWT authentication and multi-tenant access control. Every microservice API call requires a cryptographically signed SIL token.
*   **COL (Cloud Operating Layer):** The raw infrastructure execution engine. Bypassing massive orchestration frameworks like Kubernetes, COL binds directly to the local `/var/run/docker.sock` to dynamically pull images, map ports, and spin up containers.

### Phase 2: The Memory Layer (RAG & Cognitive Search)
*   **SSI (Semantic Search Infrastructure):** A hybrid keyword/vector search API wrapping a Qdrant database. It acts as the **"episodic memory"** for KalpanaOS, converting task outputs, logs, and system events into dense vector embeddings.
*   **AICP (AI Control Plane):** The intelligent brain of the operator. When an operator issues a natural language request, the AICP:
    1. Queries SSI for relevant historical context.
    2. Queries COL for live infrastructure state.
    3. Prompts the NVIDIA LLM to generate actionable infrastructure commands.

### Phase 3: The Autonomous Agent Framework (AAF)
Evolving KalpanaOS from passive control to active management.
*   **AAF Service:** A decentralized async job execution engine.
*   **Agent Fleet:**
    *   `InfraReportAgent`: Aggregates live system telemetry into plain-english reports.
    *   `MetricAnalysisAgent`: Analyzes time-series data to detect workload trends.
    *   `PredictiveScalingAgent`: Recommends proactive horizontal scaling based on historical memory constraints.
    *   `RemediationAgent`: A self-healing loop that evaluates crashing containers and automatically attempts re-configuration.

### Phase 4: Telemetry & Deep Observability
Providing the agents with "eyes" into the infrastructure.
*   **PLG Stack Integration:** Native deployment of Prometheus (metrics), Loki (logs), Tempo (traces), and Grafana (visuals).
*   **AI Diagnostics:** Agents continuously pipe Prometheus metrics and Loki crash logs into the NVIDIA LLM, allowing KalpanaOS to "read" its own error traces and trigger the `RemediationAgent`.

### Phase 5: Distributed Federation & Resilience
The final phase that elevated KalpanaOS from a single-node AI brain into a resilient, P2P federated mesh.
*   **PGE (Policy & Governance Engine):** The immutable constitutional firewall. Before `col` executes *any* deployment or deletion—whether requested by a human or an AI agent—it must pass through PGE's hard-coded rulesets.
*   **IKG (Infrastructure Knowledge Graph):** A hyper-optimized, custom in-memory adjacency list written in pure Go (consuming <20MB RAM). It passively listens to NATS events and maps the exact topological relationship between physical nodes and running services in real-time.
*   **FDCL (Federated Distributed Cognition Layer):** The mesh broker. Edge nodes gossip their local AI capabilities via a NATS broadcast (`kalpana.fdcl.gossip`). If an `aaf` instance receives a task it can't run natively, it seamlessly tunnels the request to a capable remote peer.
*   **Distributed Scheduler:** The `SchedulerAgent` evaluates live node telemetry across the entire graph (via `ikg` and Prometheus), tunneling workload deployment JSONs to the most optimal edge node over the NATS event bus.
*   **Memory Compression Lifecycle:** To prevent data bloat, the `MemoryCompressionAgent` operates as a perpetual garbage collector. It routinely fetches raw episodic logs from Qdrant, commands the LLM to synthesize them into a dense semantic "Insight", and surgically deletes the raw vectors to compress the memory footprint indefinitely.

---

## 🚀 Deployment & Installation

KalpanaOS uses `docker-compose` for rapid orchestration. It requires a Linux/macOS host with Docker installed and at least 4GB of RAM.

### 1. Environment Configuration

Clone the repository and set up your `.env` file:

```bash
git clone https://github.com/kalpanaos/kalpanaos.git
cd kalpanaos

# Create environment configuration
cat <<EOF > .env
ADMIN_EMAIL=admin@kalpana.os
ADMIN_PASSWORD=Kalpana@2026!
NVIDIA_API_KEY=your_nvidia_api_key_here
NVIDIA_API_BASE_URL=https://integrate.api.nvidia.com/v1
NVIDIA_CHAT_MODEL=meta/llama-3.1-8b-instruct
EOF
```

### 2. Bootstrapping the Mesh

Bring up the entire suite of cognitive infrastructure:

```bash
docker-compose up -d --build
```

This will launch:
*   **Core Services**: `sil` (8001), `col` (8002), `ssi` (8003), `aicp` (8004), `aaf` (8005)
*   **Federation & State**: `nats` (4222), `qdrant` (6333), `pge` (8007), `ikg` (8008), `fdcl` (8009)
*   **Telemetry**: `prometheus` (9090), `loki` (3100), `tempo` (3200), `grafana` (3000)

### 3. API Verification

Verify the system is running by checking the Federated Registry:

```bash
curl -s http://localhost:8009/registry/agents | jq
```

You should see a JSON array of all active, gossiping agents connected to your mesh.

---

## 🛠️ Interacting with KalpanaOS

You do not use a CLI to deploy apps on KalpanaOS. You ask the `AICP` or submit a task to `AAF`.

### Example: Scheduling a Deployment
Authenticate and dispatch the `SchedulerAgent` to deploy an Nginx server dynamically:

```bash
# 1. Get SIL Token
TOKEN=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{"email":"admin@kalpana.os","password":"Kalpana@2026!"}' \
  http://localhost:8001/auth/login | jq -r .access_token)

# 2. Dispatch Task
curl -s -X POST -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "agent_id": "SchedulerAgent",
    "input": "{\"name\":\"web-server\",\"image\":\"nginx:alpine\",\"ports\":[\"8080:80\"]}"
  }' \
  http://localhost:8005/tasks
```

The system will intelligently query the `IKG` for telemetry, route the deployment to the best node over NATS, and update the graph in real-time.

---

## 🔒 Security & Architecture Decisions

*   **No Local LLMs**: To strictly adhere to the 4GB RAM ceiling, all local models (Ollama/Llama3.2) were stripped out in Phase 5. KalpanaOS exclusively uses the NVIDIA API, maintaining extremely lightweight Go binaries.
*   **SQLite + CGO**: All services rely on lightweight, isolated SQLite databases. This avoids the massive memory overhead of running PostgreSQL or MySQL clusters.
*   **NATS over HTTP**: Internal service communication heavily favors NATS JetStream for pub/sub decoupling (like `kalpana.col.events` and `kalpana.fdcl.gossip`) to ensure resilience if individual nodes crash.

---

## 📜 License
KalpanaOS is an experimental research project in Autonomous Infrastructure and Distributed Cognition. Open-sourced under the MIT License.
