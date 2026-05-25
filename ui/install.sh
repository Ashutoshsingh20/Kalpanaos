#!/bin/bash
# ─── KalpanaOS Sovereign Bootstrap Installer ───

set -e

# Configuration
GITHUB_REPO="https://github.com/Ashutoshsingh20/Kalpanaos.git"
GATEWAY_URL="{{GATEWAY_URL}}"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ASCII Art
echo -e "${CYAN}"
echo "    ⚛  K A L P A N A  O S  ⚛"
echo "    Sovereign Cognitive Infrastructure"
echo "─────────────────────────────────────────"
echo -e "${NC}"

echo -e "${BLUE}[*] Initializing installation sequence...${NC}"
echo -e "${BLUE}[*] Configured gateway: ${GATEWAY_URL}${NC}\n"

# Step 1: Check requirements
echo -e "${CYAN}[1/4] Checking host environment requirements...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}[error] Docker is not installed. Please install Docker first: https://docs.docker.com/get-docker/${NC}"
    exit 1
fi
echo -e "  - Docker: ${GREEN}Installed${NC}"

if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
    echo -e "  - Docker Compose: ${GREEN}Installed (v2)${NC}"
elif docker-compose --version &> /dev/null; then
    COMPOSE_CMD="docker-compose"
    echo -e "  - Docker Compose: ${GREEN}Installed (v1)${NC}"
else
    echo -e "${RED}[error] Docker Compose is not installed. Please install docker-compose.${NC}"
    exit 1
fi

# Step 2: Clone repository
echo -e "\n${CYAN}[2/4] Pulling KalpanaOS repository...${NC}"
if [ -d "Kalpanaos" ]; then
    echo -e "  - Directory 'Kalpanaos' already exists. Navigating into it..."
    cd Kalpanaos
else
    echo -e "  - Cloning from: ${GITHUB_REPO}..."
    git clone "$GITHUB_REPO"
    cd Kalpanaos
fi

# Step 3: Setup environment
echo -e "\n${CYAN}[3/4] Preparing configuration (.env)...${NC}"
if [ ! -f .env ]; then
    echo -e "  - Copying .env.example..."
    cp .env.example .env
    
    # Prompt for key if not preset
    echo -e "\n${BLUE}[info] KalpanaOS requires an NVIDIA API Key for cognitive RAG operations."
    echo -e "Press Enter to use the default evaluation key, or enter your custom NVIDIA API Key below:${NC}"
    read -r user_key
    if [ -n "$user_key" ]; then
        # Replace key in env
        sed -i '' "s|NVIDIA_API_KEY=.*|NVIDIA_API_KEY=$user_key|g" .env 2>/dev/null || sed -i "s|NVIDIA_API_KEY=.*|NVIDIA_API_KEY=$user_key|g" .env
    fi
else
    echo -e "  - .env file already exists. Skipping config override."
fi

# Step 4: Bootstrapping Docker stack
echo -e "\n${CYAN}[4/4] Starting KalpanaOS containers...${NC}"
echo -e "  - Running '$COMPOSE_CMD up -d --build'..."
$COMPOSE_CMD up -d --build

echo -e "\n${GREEN}✓ KalpanaOS has successfully bootstrapped!${NC}"
echo -e "──────────────────────────────────────────────────────"
echo -e "  - Web Dashboard  : http://localhost:3000"
echo -e "  - Remote Tunnel  : ${GATEWAY_URL}"
echo -e "  - Default Login  : Use the admin email/password you registered during download."
echo -e "──────────────────────────────────────────────────────"
echo -e "To interact via terminal, run the CLI tool:"
echo -e "  1. Move the downloaded CLI tool 'kalpana' to a directory in your PATH (e.g. /usr/local/bin)"
echo -e "  2. Run: ${CYAN}kalpana auth login --email <your_email>${NC}"
echo ""
