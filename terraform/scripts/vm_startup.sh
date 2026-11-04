#!/bin/bash
# FinGuard VM startup script (rendered by Terraform templatefile).
# Runs on every boot -> must be idempotent.
set -euo pipefail
exec > >(tee -a /var/log/finguard-startup.log) 2>&1
echo "=== FinGuard startup: $(date -Is) ==="

REPO_URL="${repo_url}"
REPO_BRANCH="${repo_branch}"
APP_DIR="${app_dir}"
APP_USER="${app_user}"

# 1. Docker Engine + compose plugin (official apt repo)
if ! command -v docker >/dev/null 2>&1; then
  echo "Installing Docker..."
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg git
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
else
  echo "Docker already installed: $(docker --version)"
fi

# 2. App user (in docker group)
if ! id "$APP_USER" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "$APP_USER"
fi
usermod -aG docker "$APP_USER"

# 3. Clone repo (first boot only; later updates via `git pull`)
if [ ! -d "$APP_DIR/.git" ]; then
  echo "Cloning $REPO_URL ($REPO_BRANCH) -> $APP_DIR"
  git clone --branch "$REPO_BRANCH" "$REPO_URL" "$APP_DIR" || echo "WARN: git clone failed (private repo?). Clone manually."
else
  echo "Repo already exists at $APP_DIR"
fi
if [ -d "$APP_DIR" ]; then
  chown -R "$APP_USER":"$APP_USER" "$APP_DIR"
  git config --system --add safe.directory "$APP_DIR" || true
fi

echo "=== FinGuard startup done: $(date -Is) ==="

