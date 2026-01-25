# ⚠️ IMPORTANT: Virtual Environment Setup

## Quick Start

**Always use `.venv` (not `venv`):**

```bash
# Activate the correct environment
source activate.sh

# Or manually:
source .venv/bin/activate
```

## Why Two Virtual Environments?

- **`venv/`** - Old environment (NO dependencies installed) ❌
- **`.venv/`** - Current environment (ALL dependencies installed) ✅

## Initial Setup (if .venv doesn't exist)

```bash
# Create virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Verify You're Using the Correct Environment

Your prompt should show `(.venv)` not `(venv)`:

```bash
(.venv) V-Personal@MacBook-Pro FinLoom-2026 %  # ✅ Correct
(venv) V-Personal@MacBook-Pro FinLoom-2026 %   # ❌ Wrong!
```

## Remove Old venv (Optional)

If you want to avoid confusion:

```bash
# Make sure you're not in venv
deactivate

# Remove old venv directory
rm -rf venv
```
