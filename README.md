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

KalpanaOS is structured as a decentralized suite of lightweight Go microservices connected via a resilient NATS JetStream event bus and private Docker networks. The ecosystem was built iteratively across **Six Core Phases**:

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

### Phase 6: Application Compiler & Portal
- **App Compiler Service (`services/col/apps.go`):** Simulates git-based compilation pipelines to build and compile Web bundles, Android APKs, and iOS IPAs (via remote macOS peer connections) directly from private source repositories.
- **Subdomain Web Server Routing:** Supports dynamic resolution of deployed web applications via host-header matching, routing wildcard domains (`[app-id].[host].nip.io`) or custom domain records directly to static web directories.

---

## 🌐 Public Portal & Secure Download Registration

To make hosting and exposure of KalpanaOS user-friendly, the Web UI is structured into two main access routes:

### 1. Public Landing Page (`/` -> `index.html`)
The default root path serves a public landing page containing an **About Section** detailing the OS architecture, and a **Downloads Portal** offering installation utilities:
* **Bootstrapper Installer Script (`install.sh`):** A shell utility to automatically check requirements and pull the stack.
* **CLI Control Binary (`kalpana`):** The compiled terminal controller binary.

### 2. Administrative Console (`/dashboard.html`)
The main control panel, chat interface, event logs, and app compiler terminals are situated on a dedicated page (`/dashboard.html`). If a user has valid session credentials stored in their browser, they bypass the login wall automatically.

### 3. Register-on-Download Flow
Clicking any download action on the public landing page triggers a secure **Admin Registration Modal**:
* Prompts the user to create an administrative account (Email & Password).
* Submits a request to the Sovereign Identity Layer (`POST /api/sil/auth/register`), which hashes the credentials and saves the user directly in the database, mapping them to the `admin` role.
* Automatically saves the JWT access and refresh tokens to local storage.
* Initiates the browser file download.
* **Auto-Login:** Once registered, clicking "Launch Console" takes the user directly to their management dashboard at `/dashboard.html` without prompting them to log in again.

---

## 🚀 Deployment & Installation

KalpanaOS uses `docker-compose` for rapid orchestration. It requires a Linux/macOS host with Docker installed and at least 4GB of RAM.

### 1. Fast Bootstrap Installer
You can download the installer directly from your public instance URL. Simply run:
```bash
curl -sSL http://<your-public-url>/install.sh | bash
```

### 2. Exposing KalpanaOS with Zero-Config public URL
If your edge server is behind a local NAT router or firewall, the stack includes a self-healing **`tunnel`** service inside `docker-compose.yml` that opens a secure reverse tunnel using `localhost.run`.
* On startup, the container automatically establishes a secure tunnel mapping to the Nginx UI.
* You can view your generated public HTTPS address by checking the container logs:
  ```bash
  docker logs kalpana-tunnel
  ```
  **Output Box Example:**
  ```text
  ===================================================
     KALPANA OS PUBLIC TUNNEL STARTED
     Your public domain is:
     https://9a8e3a6c90721a.lhr.life
  ===================================================
  ```

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
