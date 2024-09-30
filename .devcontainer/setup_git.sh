#!/bin/bash

# Exit if any command fails
set -e

# Step 1: Authenticate with GitHub CLI
echo "🔐 Logging into GitHub CLI..."
gh auth status &> /dev/null

if [ $? -ne 0 ]; then
    echo "🌐 No GitHub authentication found. Starting GitHub CLI authentication..."
    gh auth login
else
    echo "✔️ Already authenticated with GitHub CLI."
fi

# Refresh authentication with "user" scope if needed
echo "🔄 Refreshing GitHub authentication to request 'user' scope..."
gh auth refresh -h github.com -s user

# Step 2: Fetch GitHub username and email using GitHub CLI
echo "📡 Fetching GitHub username and email..."
GITHUB_USERNAME=$(gh api user --jq '.login')
GITHUB_EMAIL=$(gh api user/emails --jq '.[] | select(.primary==true).email')

# Step 3: Set Git username and email
echo "⚙️ Configuring Git with your GitHub credentials..."
git config --global user.name "$GITHUB_USERNAME"
git config --global user.email "$GITHUB_EMAIL"

echo "✔️ Git has been configured with your GitHub username: $GITHUB_USERNAME and email: $GITHUB_EMAIL."
