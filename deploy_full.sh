#!/bin/bash

# 1. Update sermons (optional, usually run manually before this)
# python3 update_sermons.py

# 2. Generate site data (creates docs/data and docs/site_api)
echo "Generating site data..."
python3 generate_site_data.py
python3 generate_wmb_site_data.py

# 3. Sync to Cloudflare R2
echo "Syncing data to Cloudflare R2..."
./sync_to_r2.sh

# 4. Build Frontend (using .env.production)
echo "Building frontend..."
npm run build

# 5. Git Commit & Push
echo "Deploying to GitHub..."
git add .
git commit -m "Update site data and content"
git push

echo "Deployment Complete!"
