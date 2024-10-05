#!/bin/bash

echo "🔍 Starting Setup Script..."

echo "🔍 Checking if Poetry is installed..."
if ! command -v poetry &> /dev/null; then
    echo "❌ Poetry is not installed. Installing Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
    echo "✔️ Poetry has been installed."
    export PATH="$HOME/.local/bin:$PATH"
else
    echo "✔️ Poetry is already installed."
fi

# Ensure Poetry is in PATH for the current session
export PATH="$HOME/.local/bin:$PATH"

# Check if PATH is already added to ~/.bashrc
if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' "$HOME/.bashrc"; then
    echo "🔧 Adding Poetry path to ~/.bashrc..."
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
    echo "✔️ PATH has been added to ~/.bashrc."
else
    echo "✔️ Poetry path is already in ~/.bashrc."
fi

# Reload ~/.bashrc to apply the PATH change for the current session
source "$HOME/.bashrc"

echo "📦 Installing dependencies with Poetry..."

# Verify Poetry installation
poetry --version

# Update the lock file without updating dependencies
poetry lock --no-update

# Install dependencies
poetry install --no-interaction --no-ansi

echo "✅ Setup completed successfully."
