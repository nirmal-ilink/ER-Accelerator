#!/bin/bash
# ============================================
# Databricks Apps Build & Deploy Script
# ============================================
# This script packages and deploys the ER-Accelerator
# as a Databricks App for managed hosting.
#
# Prerequisites:
#   - Databricks CLI installed and configured
#   - Python 3.12+ (matching Databricks Runtime)
#   - Valid Databricks workspace access
#
# Usage:
#   chmod +x databricks_app_build.sh
#   ./databricks_app_build.sh
# ============================================

set -e  # Exit on error

echo "=========================================="
echo "ER-Accelerator Databricks App Deployment"
echo "=========================================="

# Configuration
APP_NAME="er-accelerator"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${APP_DIR}/.databricks_build"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Step 1: Validating environment...${NC}"

# Check Databricks CLI
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}ERROR: Databricks CLI not found.${NC}"
    echo "Install with: pip install databricks-cli"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
if [[ "$PYTHON_VERSION" != "3.12" ]]; then
    echo -e "${YELLOW}WARNING: Python $PYTHON_VERSION detected. Databricks Connect requires Python 3.12.${NC}"
fi

echo -e "${GREEN}✓ Environment validated${NC}"

echo -e "${YELLOW}Step 2: Preparing build directory...${NC}"

# Clean and create build directory
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

# Copy application files
cp -r "${APP_DIR}/src" "${BUILD_DIR}/"
cp -r "${APP_DIR}/config" "${BUILD_DIR}/"
cp -r "${APP_DIR}/data" "${BUILD_DIR}/" 2>/dev/null || mkdir -p "${BUILD_DIR}/data"
cp "${APP_DIR}/requirements.txt" "${BUILD_DIR}/"

# Create app.yaml for Databricks Apps
cat > "${BUILD_DIR}/app.yaml" << EOF
# Databricks App Configuration
name: ${APP_NAME}
description: "iCORE - Enterprise Entity Resolution Accelerator"

# Entry point
command: "streamlit run src/frontend/app.py --server.port=\${port} --server.address=0.0.0.0"

# Environment variables (secrets should be referenced, not hardcoded)
env:
  - name: DATABRICKS_HOST
    valueFrom:
      secretRef: er-accelerator/databricks-host
  - name: DATABRICKS_TOKEN
    valueFrom:
      secretRef: er-accelerator/databricks-token
  - name: DATABRICKS_CLUSTER_ID
    valueFrom:
      secretRef: er-accelerator/cluster-id

# Resources
resources:
  num_cpus: 2
  memory_mb: 4096
EOF

echo -e "${GREEN}✓ Build directory prepared${NC}"

echo -e "${YELLOW}Step 3: Creating Databricks secrets (if not exists)...${NC}"

# Check if secret scope exists, create if not
SCOPE_NAME="er-accelerator"
if ! databricks secrets list-scopes 2>/dev/null | grep -q "${SCOPE_NAME}"; then
    echo "Creating secret scope: ${SCOPE_NAME}"
    databricks secrets create-scope --scope "${SCOPE_NAME}" || true
fi

echo -e "${GREEN}✓ Secret scope ready${NC}"
echo -e "${YELLOW}NOTE: Remember to set secrets using:${NC}"
echo "  databricks secrets put --scope ${SCOPE_NAME} --key databricks-host"
echo "  databricks secrets put --scope ${SCOPE_NAME} --key databricks-token"
echo "  databricks secrets put --scope ${SCOPE_NAME} --key cluster-id"

echo -e "${YELLOW}Step 4: Deploying to Databricks Apps...${NC}"

# Deploy using Databricks CLI
cd "${BUILD_DIR}"
databricks apps deploy "${APP_NAME}" --source-code-path . || {
    echo -e "${RED}ERROR: Deployment failed.${NC}"
    echo "Make sure you have the correct permissions and the Databricks Apps feature is enabled."
    exit 1
}

echo -e "${GREEN}=========================================="
echo "✓ Deployment Complete!"
echo "==========================================${NC}"
echo ""
echo "Your app is being deployed. Check the Databricks workspace for status."
echo "Once deployed, access your app at:"
echo "  https://<your-workspace>.azuredatabricks.net/apps/${APP_NAME}"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Set your secrets in Databricks Secret Scope '${SCOPE_NAME}'"
echo "2. Start the app from the Databricks Apps UI"
echo "3. Share the app URL with your users"
