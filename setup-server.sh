#!/bin/bash

# MCAP Tracker Server Setup Script
# Run this on your Ubuntu VPS after connecting via SSH

set -e

echo "=========================================="
echo "MCAP Tracker Server Setup"
echo "=========================================="

# Update system
echo "[1/8] Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Node.js
echo "[2/8] Installing Node.js..."
if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
    sudo apt-get install -y nodejs
else
    echo "Node.js already installed: $(node --version)"
fi

# Install Nginx
echo "[3/8] Installing Nginx..."
if ! command -v nginx &> /dev/null; then
    sudo apt install nginx -y
    sudo systemctl start nginx
    sudo systemctl enable nginx
else
    echo "Nginx already installed"
fi

# Install PM2
echo "[4/8] Installing PM2..."
if ! command -v pm2 &> /dev/null; then
    sudo npm install -g pm2
else
    echo "PM2 already installed: $(pm2 --version)"
fi

# Install Certbot
echo "[5/8] Installing Certbot..."
if ! command -v certbot &> /dev/null; then
    sudo apt install certbot python3-certbot-nginx -y
else
    echo "Certbot already installed"
fi

# Install application dependencies
echo "[6/8] Installing application dependencies..."
if [ -f "package.json" ]; then
    npm install --production
else
    echo "WARNING: package.json not found. Make sure you're in the mcap-tracker directory."
fi

# Create Nginx configuration
echo "[7/8] Creating Nginx configuration..."
read -p "Enter your subdomain (e.g., mcap-tracker): " SUBDOMAIN
read -p "Enter your domain (e.g., agenttra.com): " DOMAIN

FULL_DOMAIN="${SUBDOMAIN}.${DOMAIN}"

sudo tee /etc/nginx/sites-available/mcap-tracker > /dev/null <<EOF
server {
    listen 80;
    server_name ${FULL_DOMAIN};

    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host \$host;
        proxy_cache_bypass \$http_upgrade;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

# Enable site
sudo ln -sf /etc/nginx/sites-available/mcap-tracker /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# Configure firewall
echo "[8/8] Configuring firewall..."
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
echo "y" | sudo ufw enable

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Add DNS A record in Namecheap:"
echo "   - Host: ${SUBDOMAIN}"
echo "   - Value: $(curl -s ifconfig.me)"
echo "   - TTL: Automatic"
echo ""
echo "2. Start the application:"
echo "   pm2 start server.js --name mcap-tracker"
echo "   pm2 save"
echo "   pm2 startup"
echo ""
echo "3. After DNS propagates (5-30 min), get SSL certificate:"
echo "   sudo certbot --nginx -d ${FULL_DOMAIN}"
echo ""
echo "4. Update your Python bot with:"
echo "   MCAP_SERVER_URL=https://${FULL_DOMAIN}"
echo ""
echo "Your server will be available at: https://${FULL_DOMAIN}"
echo "=========================================="

