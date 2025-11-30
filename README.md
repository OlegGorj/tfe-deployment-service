# TFE Deployment Service

A Go-based microservice that orchestrates Terraform deployments via Terraform Enterprise (TFE) backend using Temporal workflows.

## Overview

This service provides a robust, scalable, and fault-tolerant solution for managing infrastructure deployments across multiple environments. It uses Temporal workflows for reliable orchestration and supports lifecycle hooks for custom automation.

## Features

- **RESTful API** endpoints for deployment operations
- **Temporal-based** workflow orchestration for reliability and fault tolerance
- **In-memory database** for deployment state management
- **Application registry** for environment-specific configurations
- **TFE integration** for workspace and run management
- **Run lifecycle hooks** (post-plan, pre-apply, post-apply)
- **Multiple run types**: plan, plan-apply, plan-destroy, destroy

## Project Structure

```
tfe-deployment-service/
├── api/
│   ├── gen/                    # Generated Go code from proto
│   │   └── deployment/v1/
│   └── proto/                  # Protocol buffer definitions
│       └── deployment/v1/
├── cmd/
│   └── service/
│       └── main.go             # Application entry point
├── docs/
│   └── design.md               # Design document
├── internal/
│   └── config/                 # Configuration management
├── pkg/
│   ├── activities/             # Temporal activities
│   ├── db/                     # In-memory database
│   ├── handlers/               # HTTP handlers
│   ├── tfe/                    # TFE client
│   └── workflows/              # Temporal workflows
├── go.mod
├── go.sum
└── README.md
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/deploy` | Create a new deployment |
| GET | `/status/:id` | Get deployment status |
| POST | `/callback` | Handle TFE callbacks |
| GET | `/poll/:id` | Poll deployment status |
| GET | `/health` | Health check |
| GET | `/ready` | Readiness check |

## Quick Start

### Prerequisites

- Go 1.21+
- Temporal Server (local or cloud)
- TFE/Terraform Cloud account (optional for full functionality)

### Build

```bash
go build -o tfe-deployment-service ./cmd/service
```

### Run

```bash
# Set environment variables
export TEMPORAL_HOST=localhost:7233
export TFE_TOKEN=your-tfe-token
export TFE_ORGANIZATION=your-org

# Run the service
./tfe-deployment-service
```

### Example Deploy Request

```bash
curl -X POST http://localhost:8080/deploy \
  -H "Content-Type: application/json" \
  -d '{
    "git_repo": "https://github.com/your-org/your-repo",
    "branch": "main",
    "app_name": "webapp",
    "environment": "dev",
    "run_type": "plan-apply"
  }'
```

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `SERVER_PORT` | HTTP server port | 8080 |
| `TEMPORAL_HOST` | Temporal server address | localhost:7233 |
| `TEMPORAL_NAMESPACE` | Temporal namespace | default |
| `TEMPORAL_TASK_QUEUE` | Temporal task queue | tfe-deployment-queue |
| `TFE_ADDRESS` | TFE API address | https://app.terraform.io |
| `TFE_TOKEN` | TFE API token | - |
| `TFE_ORGANIZATION` | Default TFE organization | - |

## Documentation

See [docs/design.md](docs/design.md) for detailed design documentation including:
- Architecture diagrams
- Data models
- Workflow descriptions
- Internal registry structure
- Run hooks configuration

## License

MIT