#!/bin/bash
# FinLoom Virtual Environment Activation Script
# 
# Usage: source activate.sh
#
# This ensures you're using the correct virtual environment (.venv)
# with all dependencies installed.

if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "✅ Activated .venv (with all dependencies)"
    echo ""
    echo "Python: $(which python)"
    echo "Version: $(python --version)"
    echo ""
else
    echo "❌ Error: .venv directory not found!"
    echo "Please create it first:"
    echo "  python3 -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  pip install -r requirements.txt"
fi
