# Demo OTLP Ingest Script

This folder contains a Python demo generator that bootstraps a demo team and user, then sends traces, logs, and metrics to Optikk over OTLP gRPC.

## Files

- `demo_otlp_ingest.py`: creates or reuses a demo team/user and emits realistic telemetry
- `requirements-demo.txt`: Python packages needed by the script

## Quick Start

If you are already inside this `scripts/` folder:

```bash
python3 -m venv .venv-demo
source .venv-demo/bin/activate
python -m pip install -r requirements-demo.txt
python demo_otlp_ingest.py --once
```

If you prefer running from the repo root:

```bash
cd /Users/ramantayal/pro/optikk-backend
python3 -m venv scripts/.venv-demo
source scripts/.venv-demo/bin/activate
python -m pip install -r scripts/requirements-demo.txt
python scripts/demo_otlp_ingest.py --once
```

## What It Does

- creates or reuses a demo team via `POST /api/v1/teams`
- creates a demo user via `POST /api/v1/users`
- verifies login via `POST /api/v1/auth/login`
- sends OTLP traces, logs, and metrics to `localhost:4317` by default

## Default Login

By default the script uses:

- email: `demo@optikk.local`
- password: `DemoPass123!`

The script prints the login credentials on startup and stores them in the state file.

## Default State File

The bootstrap state is stored at:

```text
/tmp/optikk-demo-team.json
```

It includes:

- `team_id`
- `api_key`
- `org_name`
- `team_name`
- `created_at`
- `user_id`
- `user_email`
- `user_password`
- `user_name`

## Useful Flags

```bash
python demo_otlp_ingest.py --help
```

Common ones:

- `--once`: send one batch and exit
- `--reset-team`: ignore saved state and create a fresh team/user
- `--backend-http`: backend HTTP URL, default `http://localhost:9090`
- `--backend-grpc`: OTLP gRPC endpoint, default `localhost:4317`
- `--user-email`: override demo login email
- `--user-password`: override demo login password

## Continuous Demo Mode

To keep dashboards fresh for screenshots:

```bash
python demo_otlp_ingest.py
```

## Notes

- Use a virtualenv; do not install these packages into the system Python on macOS Homebrew Python.
- `opentelemetry-proto` and `grpcio` are Python packages, not Homebrew formulas.
