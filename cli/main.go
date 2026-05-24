package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ─── Config ───────────────────────────────────────────────────────────────────

var cfgFile string

func baseURL(service string) string {
	ports := map[string]string{
		"sil":  "8001",
		"col":  "8002",
		"ssi":  "8003",
		"aicp": "8004",
		"aaf":  "8005",
	}
	host := viper.GetString("host")
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s:%s", host, ports[service])
}

func token() string {
	return viper.GetString("token")
}

// ─── HTTP helpers ──────────────────────────────────────────────────────────────

func doRequest(method, url string, body interface{}) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if tok := token(); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return data, resp.StatusCode, nil
}

func printJSON(data []byte) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Println(string(data))
		return
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func fail(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "✕ "+msg+"\n", args...)
	os.Exit(1)
}

// ─── Root ──────────────────────────────────────────────────────────────────────

var rootCmd = &cobra.Command{
	Use:   "kalpana",
	Short: "KalpanaOS CLI — sovereign AI cloud management",
	Long: `kalpana is the command-line interface for KalpanaOS.
It lets you manage services, chat with KalpanaAI, run agents,
search your knowledge base, and monitor your infrastructure.

Configure with: kalpana config set host <server-ip>
`,
}

// ─── Config command ────────────────────────────────────────────────────────────

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage CLI configuration",
}

var configSetCmd = &cobra.Command{
	Use:   "set <key> <value>",
	Short: "Set a configuration value",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		viper.Set(args[0], args[1])
		if err := viper.WriteConfig(); err != nil {
			_ = viper.SafeWriteConfig()
		}
		fmt.Printf("✓ Set %s = %s\n", args[0], args[1])
	},
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current configuration",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("host:  %s\n", viper.GetString("host"))
		tok := viper.GetString("token")
		if tok != "" {
			fmt.Printf("token: %s…\n", tok[:min(20, len(tok))])
		} else {
			fmt.Println("token: (not set — run 'kalpana auth login')")
		}
	},
}

// ─── Auth command ──────────────────────────────────────────────────────────────

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Authentication commands",
}

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate with KalpanaOS",
	Run: func(cmd *cobra.Command, args []string) {
		email, _ := cmd.Flags().GetString("email")
		pass, _ := cmd.Flags().GetString("password")
		if email == "" || pass == "" {
			fail("--email and --password are required")
		}

		data, status, err := doRequest("POST", baseURL("sil")+"/auth/login", map[string]string{
			"email": email, "password": pass,
		})
		if err != nil {
			fail("Cannot reach SIL service: %v", err)
		}
		if status != 200 {
			fail("Login failed (HTTP %d): %s", status, string(data))
		}

		var resp map[string]interface{}
		json.Unmarshal(data, &resp)
		tok, _ := resp["access_token"].(string)
		ref, _ := resp["refresh_token"].(string)

		viper.Set("token", tok)
		viper.Set("refresh_token", ref)
		if err := viper.WriteConfig(); err != nil {
			_ = viper.SafeWriteConfig()
		}

		fmt.Println("✓ Authenticated successfully")
		if user, ok := resp["user"].(map[string]interface{}); ok {
			fmt.Printf("  Logged in as: %v\n", user["email"])
		}
	},
}

var whoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Show current user",
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("GET", baseURL("sil")+"/auth/me", nil)
		if err != nil || status != 200 {
			fail("Failed: %v / HTTP %d", err, status)
		}
		printJSON(data)
	},
}

// ─── Services command ──────────────────────────────────────────────────────────

var servicesCmd = &cobra.Command{
	Use:   "services",
	Short: "Manage infrastructure services",
}

var servicesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all running services",
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("GET", baseURL("col")+"/services", nil)
		if err != nil || status != 200 {
			fail("Failed: %v / HTTP %d\n%s", err, status, string(data))
		}

		var services []map[string]interface{}
		json.Unmarshal(data, &services)

		if len(services) == 0 {
			fmt.Println("No services deployed.")
			return
		}

		fmt.Printf("%-30s %-20s %-12s\n", "NAME", "IMAGE", "STATUS")
		fmt.Println(strings.Repeat("─", 65))
		for _, s := range services {
			name := fmt.Sprint(s["name"])
			image := fmt.Sprint(s["image"])
			status := fmt.Sprint(s["status"])
			if image == "<nil>" {
				image = fmt.Sprint(s["Image"])
			}
			statusSymbol := "●"
			if status == "running" {
				statusSymbol = "✓"
			}
			fmt.Printf("%-30s %-20s %s %s\n", name, image, statusSymbol, status)
		}
	},
}

var servicesDeployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy a service",
	Run: func(cmd *cobra.Command, args []string) {
		name, _ := cmd.Flags().GetString("name")
		image, _ := cmd.Flags().GetString("image")
		ports, _ := cmd.Flags().GetStringSlice("port")
		envs, _ := cmd.Flags().GetStringSlice("env")
		mem, _ := cmd.Flags().GetString("mem")

		if name == "" || image == "" {
			fail("--name and --image are required")
		}

		manifest := map[string]interface{}{
			"name": name, "image": image,
			"ports": ports, "env": envs, "mem_limit": mem,
		}
		data, status, err := doRequest("POST", baseURL("col")+"/services/deploy", manifest)
		if err != nil || status >= 400 {
			fail("Deploy failed (HTTP %d): %s", status, string(data))
		}
		fmt.Printf("✓ Service \"%s\" deployed\n", name)
		printJSON(data)
	},
}

var servicesStopCmd = &cobra.Command{
	Use:   "stop <name>",
	Short: "Stop a service",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		data, status, err := doRequest("DELETE", baseURL("col")+"/services/"+name, nil)
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		fmt.Printf("✓ Service \"%s\" stopped\n", name)
	},
}

// ─── Chat command ──────────────────────────────────────────────────────────────

var chatCmd = &cobra.Command{
	Use:   "chat <message>",
	Short: "Chat with KalpanaAI",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		message := strings.Join(args, " ")
		fmt.Printf("You: %s\n\n", message)
		fmt.Print("KalpanaAI: ")

		data, status, err := doRequest("POST", baseURL("aicp")+"/chat", map[string]string{
			"message": message,
		})
		if err != nil || status >= 400 {
			fail("Chat failed (HTTP %d): %s", status, string(data))
		}

		var resp map[string]interface{}
		json.Unmarshal(data, &resp)
		reply, _ := resp["reply"].(string)
		fmt.Println(reply)

		if pending, ok := resp["pending_command"].(map[string]interface{}); ok {
			fmt.Printf("\n⚠ Pending confirmation required:\n  %v\n", pending["description"])
			fmt.Printf("  ID: %v\n", pending["id"])
			fmt.Println("\n  Run: kalpana confirm <id>  OR  kalpana reject <id>")
		}
	},
}

var confirmCmd = &cobra.Command{
	Use:   "confirm <pending-id>",
	Short: "Confirm a pending command",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("POST", baseURL("aicp")+"/pending/"+args[0]+"/confirm", nil)
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		fmt.Println("✓ Command confirmed and executed")
		printJSON(data)
	},
}

var rejectCmd = &cobra.Command{
	Use:   "reject <pending-id>",
	Short: "Reject a pending command",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("POST", baseURL("aicp")+"/pending/"+args[0]+"/reject", nil)
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		fmt.Println("✓ Command rejected")
		printJSON(data)
	},
}

// ─── Agents command ────────────────────────────────────────────────────────────

var agentsCmd = &cobra.Command{
	Use:   "agents",
	Short: "Manage autonomous agents",
}

var agentsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List registered agents",
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("GET", baseURL("aaf")+"/agents", nil)
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		var agents []map[string]interface{}
		json.Unmarshal(data, &agents)
		for _, a := range agents {
			fmt.Printf("  ◎ %-30s %s\n", a["name"], a["description"])
		}
	},
}

var agentsRunCmd = &cobra.Command{
	Use:   "run <agent-id>",
	Short: "Run an agent",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		input, _ := cmd.Flags().GetString("input")
		if input == "" {
			input = "Execute default task"
		}
		data, status, err := doRequest("POST", baseURL("aaf")+"/tasks", map[string]string{
			"agent_id": args[0], "input": input,
		})
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		fmt.Println("✓ Agent task submitted")
		printJSON(data)
	},
}

var tasksCmd = &cobra.Command{
	Use:   "tasks",
	Short: "List agent tasks",
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("GET", baseURL("aaf")+"/tasks", nil)
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		var tasks []map[string]interface{}
		json.Unmarshal(data, &tasks)
		if len(tasks) == 0 {
			fmt.Println("No tasks yet.")
			return
		}
		fmt.Printf("%-30s %-20s %-12s\n", "ID", "AGENT", "STATUS")
		fmt.Println(strings.Repeat("─", 65))
		for _, t := range tasks {
			fmt.Printf("%-30s %-20s %s\n", t["id"], t["agent_id"], t["status"])
		}
	},
}

// ─── Search command ────────────────────────────────────────────────────────────

var searchCmd = &cobra.Command{
	Use:   "search <query>",
	Short: "Search the knowledge base",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		query := strings.Join(args, " ")
		topK, _ := cmd.Flags().GetInt("top")

		data, status, err := doRequest("POST", baseURL("ssi")+"/search", map[string]interface{}{
			"query": query, "top_k": topK,
		})
		if err != nil || status >= 400 {
			fail("Search failed (HTTP %d): %s", status, string(data))
		}
		var resp map[string]interface{}
		json.Unmarshal(data, &resp)

		results, _ := resp["results"].([]interface{})
		if len(results) == 0 {
			fmt.Println("No results found. Try ingesting some documents first.")
			return
		}
		for i, r := range results {
			result, _ := r.(map[string]interface{})
			source := fmt.Sprint(result["source"])
			text := fmt.Sprint(result["text"])
			score := fmt.Sprint(result["score"])
			if len(text) > 200 {
				text = text[:200] + "…"
			}
			fmt.Printf("\n[%d] %s (score: %s)\n%s\n", i+1, source, score, text)
		}
	},
}

// ─── Status command ────────────────────────────────────────────────────────────

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show system health status",
	Run: func(cmd *cobra.Command, args []string) {
		services := []struct{ name, key string }{
			{"SIL Identity", "sil"},
			{"COL Orchestrator", "col"},
			{"SSI Search", "ssi"},
			{"AICP Control Plane", "aicp"},
			{"AAF Agent Framework", "aaf"},
		}
		fmt.Println("KalpanaOS Service Status")
		fmt.Println(strings.Repeat("─", 45))
		for _, svc := range services {
			data, status, err := doRequest("GET", baseURL(svc.key)+"/health", nil)
			symbol := "✓"
			statusStr := "OK"
			if err != nil || status != 200 {
				symbol = "✕"
				statusStr = "UNREACHABLE"
				if status > 0 {
					statusStr = fmt.Sprintf("HTTP %d", status)
				}
				_ = data
			}
			fmt.Printf("  %s  %-25s %s\n", symbol, svc.name, statusStr)
		}
	},
}

// ─── Node command ──────────────────────────────────────────────────────────────

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Show node information",
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("GET", baseURL("col")+"/nodes", nil)
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		printJSON(data)
	},
}

// ─── Audit command ─────────────────────────────────────────────────────────────

var auditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Show infrastructure audit log",
	Run: func(cmd *cobra.Command, args []string) {
		data, status, err := doRequest("GET", baseURL("col")+"/audit", nil)
		if err != nil || status >= 400 {
			fail("Failed (HTTP %d): %s", status, string(data))
		}
		var logs []map[string]interface{}
		json.Unmarshal(data, &logs)
		if len(logs) == 0 {
			fmt.Println("No audit events.")
			return
		}
		fmt.Printf("%-20s %-20s %-12s %s\n", "TIME", "OPERATOR", "ACTION", "DETAIL")
		fmt.Println(strings.Repeat("─", 80))
		for _, l := range logs {
			ts := fmt.Sprint(l["ts"])
			if len(ts) > 19 {
				ts = ts[:19]
			}
			fmt.Printf("%-20s %-20s %-12s %s\n",
				ts, l["operator"], l["action"], l["detail"])
		}
	},
}

// ─── Main ──────────────────────────────────────────────────────────────────────

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func initConfig() {
	viper.SetConfigName(".kalpana")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvPrefix("KALPANA")
	_ = viper.ReadInConfig()
}

func main() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default $HOME/.kalpana.yaml)")
	rootCmd.PersistentFlags().StringP("host", "H", "localhost", "KalpanaOS server host")
	viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))

	// Auth
	loginCmd.Flags().StringP("email", "e", "", "Email address")
	loginCmd.Flags().StringP("password", "p", "", "Password")
	authCmd.AddCommand(loginCmd, whoamiCmd)

	// Config
	configCmd.AddCommand(configSetCmd, configShowCmd)

	// Services
	servicesDeployCmd.Flags().StringP("name", "n", "", "Service name (required)")
	servicesDeployCmd.Flags().StringP("image", "i", "", "Docker image (required)")
	servicesDeployCmd.Flags().StringSliceP("port", "p", nil, "Port mapping (e.g. 8080:80)")
	servicesDeployCmd.Flags().StringSliceP("env", "e", nil, "Environment vars (e.g. KEY=val)")
	servicesDeployCmd.Flags().StringP("mem", "m", "128m", "Memory limit")
	servicesCmd.AddCommand(servicesListCmd, servicesDeployCmd, servicesStopCmd)

	// Agents
	agentsRunCmd.Flags().StringP("input", "i", "", "Task input")
	agentsCmd.AddCommand(agentsListCmd, agentsRunCmd)

	// Search
	searchCmd.Flags().IntP("top", "k", 5, "Number of results")

	rootCmd.AddCommand(
		authCmd, configCmd, servicesCmd, agentsCmd,
		chatCmd, confirmCmd, rejectCmd,
		searchCmd, statusCmd, nodeCmd, auditCmd, tasksCmd,
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
