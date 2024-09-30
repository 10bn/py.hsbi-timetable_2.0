#!/bin/bash

# Exit if any command fails
set -e

echo "🔍 Starting Setup Script..."

echo "🔍 Checking if Poetry is installed..."
if ! command -v poetry &> /dev/null; then
    echo "❌ Poetry is not installed. Installing Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
    export PATH="$HOME/.local/bin:$PATH"
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
    echo "✔️ Poetry has been installed."
fi

echo "📦 Installing dependencies with Poetry..."
poetry install

echo "🔄 Activating the Poetry virtual environment..."
poetry shell

echo "📝 Setting up configuration files..."
if [ ! -f "config/config.yaml" ]; then
    cp config/config-sample.yaml config/config.yaml
    echo "✔️ config.yaml has been created. Please update it as needed."
else
    echo "✔️ config.yaml already exists. Skipping copy."
fi

echo "✅ Setup completed successfully!"
echo "🚀 To start the application, run 'python src/main.py'"
