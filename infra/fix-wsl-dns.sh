#!/bin/bash
# Fix WSL2 DNS to use Google/Cloudflare public DNS instead of corporate DNS.
# This allows Docker to reach Docker Hub when on an external network (e.g. hotspot).

set -e

echo "=== Disabling WSL auto-generated resolv.conf ==="
cat > /tmp/wsl.conf << 'EOF'
[network]
generateResolvConf = false
EOF
sudo cp /tmp/wsl.conf /etc/wsl.conf

echo "=== Setting public DNS (8.8.8.8 / 1.1.1.1) ==="
sudo rm -f /etc/resolv.conf
cat > /tmp/resolv.conf << 'EOF'
nameserver 8.8.8.8
nameserver 1.1.1.1
EOF
sudo cp /tmp/resolv.conf /etc/resolv.conf

echo "=== Configuring Docker daemon DNS ==="
cat > /tmp/daemon.json << 'EOF'
{
  "dns": ["8.8.8.8", "1.1.1.1"]
}
EOF
sudo cp /tmp/daemon.json /etc/docker/daemon.json

echo "=== Restarting Docker ==="
sudo service docker restart
sleep 3

echo "=== Testing connectivity ==="
nslookup registry-1.docker.io | head -4
echo ""
docker pull hello-world 2>&1 | tail -3
echo ""
echo "=== Done! Now run: docker-compose pull ==="
