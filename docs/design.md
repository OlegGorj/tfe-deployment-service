# TFE Deployment Service - Design Document

## 1. Overview

The TFE Deployment Service is a Go-based microservice that orchestrates Terraform deployments via Terraform Enterprise (TFE) backend using Temporal workflows. It provides a robust, scalable, and fault-tolerant solution for managing infrastructure deployments across multiple environments.

### 1.1 Purpose

- Automate Terraform deployments through TFE
- Provide a unified API for triggering and monitoring deployments
- Manage workspace lifecycle (create, configure, run)
- Support multiple run types: plan, apply, destroy
- Track deployment state with unique identifiers
- Enable application onboarding with registry-based configuration

### 1.2 Key Features

- RESTful API endpoints for deployment operations
- Temporal-based workflow orchestration for reliability
- In-memory database for deployment state management
- Application registry for environment-specific configurations
- TFE integration for workspace and run management

---

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TFE Deployment Service                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │   REST API   │───▶│   Handlers   │───▶│   Database   │                   │
│  │  (Gin HTTP)  │    │              │    │  (In-Memory) │                   │
│  └──────────────┘    └──────┬───────┘    └──────────────┘                   │
│                             │                                                │
│                             ▼                                                │
│                    ┌──────────────────┐                                      │
│                    │  Temporal Client │                                      │
│                    └────────┬─────────┘                                      │
│                             │                                                │
└─────────────────────────────┼────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Temporal Server                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                     DeploymentWorkflow                              │     │
│  │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐              │     │
│  │  │  Workspace  │──▶│   TF Run    │──▶│   Status    │              │     │
│  │  │  Activity   │   │  Activity   │   │   Update    │              │     │
│  │  └─────────────┘   └─────────────┘   └─────────────┘              │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Terraform Enterprise (TFE)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │  Workspaces  │    │     Runs     │    │    Plans     │                   │
│  └──────────────┘    └──────────────┘    └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TFE Deployment Service                        │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                         API Layer                            │    │
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐   │    │
│  │  │POST/deploy│ │GET/status │ │POST/      │ │GET/poll   │   │    │
│  │  │           │ │/:id       │ │callback   │ │/:id       │   │    │
│  │  └───────────┘ └───────────┘ └───────────┘ └───────────┘   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                      Handler Layer                           │    │
│  │  ┌─────────────────┐  ┌─────────────────────────────────┐   │    │
│  │  │ DeployHandler   │  │ StatusHandler/PollHandler       │   │    │
│  │  │ - Validate req  │  │ - Query deployment state        │   │    │
│  │  │ - Lookup registry│ │ - Return status response        │   │    │
│  │  │ - Start workflow │  │                                 │   │    │
│  │  └─────────────────┘  └─────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                     Database Layer                           │    │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐   │    │
│  │  │ Deployments │ │ Workspaces  │ │ AppRegistry         │   │    │
│  │  │ Table       │ │ Table       │ │ Table               │   │    │
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    Workflow Layer                            │    │
│  │  ┌─────────────────────────────────────────────────────┐    │    │
│  │  │              DeploymentWorkflow                      │    │    │
│  │  │  ┌──────────────┐  ┌──────────────┐                 │    │    │
│  │  │  │ Workspace    │  │ TFRun        │                 │    │    │
│  │  │  │ Activity     │  │ Activity     │                 │    │    │
│  │  │  └──────────────┘  └──────────────┘                 │    │    │
│  │  └─────────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                      TFE Client                              │    │
│  │  ┌─────────────────┐  ┌─────────────────────────────────┐   │    │
│  │  │ CreateWorkspace │  │ CreateRun / PollRunStatus       │   │    │
│  │  └─────────────────┘  └─────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Models

### 3.1 Deployment

Represents a single deployment request and tracks its lifecycle.

| Field | Type | Description |
|-------|------|-------------|
| id | string (UUID) | Unique identifier for the deployment |
| git_repo | string | Git repository URL for the Terraform code |
| branch | string | Git branch to deploy |
| app_name | string | Application name for registry lookup |
| environment | string | Target environment (dev, staging, prod) |
| run_type | enum | Type of run: plan, plan-apply, plan-destroy, destroy |
| status | enum | Current status: pending, running, success, failed |
| workspace_id | string | Associated TFE workspace ID |
| tfe_run_id | string | TFE run ID |
| message | string | Status message or error details |
| created_at | timestamp | Creation timestamp |
| updated_at | timestamp | Last update timestamp |

### 3.2 Workspace

Represents a TFE workspace configuration.

| Field | Type | Description |
|-------|------|-------------|
| id | string (UUID) | Internal workspace ID |
| name | string | Workspace name |
| tfe_workspace_id | string | TFE workspace identifier |
| organization | string | TFE organization |
| app_name | string | Associated application name |
| environment | string | Target environment |
| created_at | timestamp | Creation timestamp |
| updated_at | timestamp | Last update timestamp |

### 3.3 AppRegistry

Contains onboarding configuration for applications per environment.

| Field | Type | Description |
|-------|------|-------------|
| id | string (UUID) | Registry entry ID |
| app_name | string | Application name (lookup key) |
| environment | string | Environment (lookup key) |
| azure_identity | string | Azure managed identity |
| subscription_id | string | Azure subscription ID |
| resource_group | string | Azure resource group name |
| tenant_id | string | Azure tenant ID |
| workspace_config | object | TFE workspace configuration |
| run_hooks | object | TFE run lifecycle hooks |
| variables | map | Environment-specific variables |
| tags | map | Resource tags for the deployment |
| notification_config | object | Notification settings |
| created_at | timestamp | Creation timestamp |
| updated_at | timestamp | Last update timestamp |

### 3.4 WorkspaceConfig

Nested configuration for TFE workspace settings.

| Field | Type | Description |
|-------|------|-------------|
| organization | string | TFE organization name |
| workspace_prefix | string | Prefix for workspace naming |
| terraform_version | string | Terraform version to use |
| working_directory | string | Directory containing TF code |
| auto_apply | bool | Auto-apply after successful plan |
| vcs_repo | string | VCS repository identifier |
| execution_mode | string | Execution mode: remote, local, agent |
| agent_pool_id | string | Agent pool ID for agent execution |

### 3.5 RunHooks

Configuration for TFE run lifecycle hooks (post-plan, pre-apply, post-apply).

| Field | Type | Description |
|-------|------|-------------|
| post_plan | HookConfig | Hook executed after plan completes |
| pre_apply | HookConfig | Hook executed before apply starts |
| post_apply | HookConfig | Hook executed after apply completes |

### 3.6 HookConfig

Individual hook configuration.

| Field | Type | Description |
|-------|------|-------------|
| enabled | bool | Whether the hook is enabled |
| webhook_url | string | URL to call when hook triggers |
| webhook_method | string | HTTP method (GET, POST) |
| webhook_headers | map | Custom headers for webhook |
| webhook_payload_template | string | Template for webhook payload |
| script_path | string | Path to script in repo to execute |
| timeout_seconds | int | Timeout for hook execution |
| fail_on_error | bool | Whether to fail deployment if hook fails |

### 3.7 NotificationConfig

Notification settings for deployment events.

| Field | Type | Description |
|-------|------|-------------|
| slack_webhook_url | string | Slack webhook for notifications |
| teams_webhook_url | string | MS Teams webhook for notifications |
| email_recipients | []string | Email addresses for notifications |
| notify_on_success | bool | Send notification on success |
| notify_on_failure | bool | Send notification on failure |
| notify_on_pending_approval | bool | Send notification when approval needed |

---

## 4. API Endpoints

### 4.1 POST /deploy

Creates a new deployment request.

**Request:**
```json
{
  "git_repo": "https://github.com/org/repo",
  "branch": "main",
  "app_name": "webapp",
  "environment": "dev",
  "run_type": "plan-apply"
}
```

**Response:**
```json
{
  "deployment_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Deployment initiated successfully"
}
```

**Flow:**
1. Validate request parameters
2. Lookup app configuration in registry (app_name + environment)
3. Generate unique deployment ID (UUID)
4. Create deployment record with status "pending"
5. Start Temporal DeploymentWorkflow
6. Return deployment ID to client

### 4.2 GET /status/:id

Retrieves detailed status of a deployment.

**Response:**
```json
{
  "deployment_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "git_repo": "https://github.com/org/repo",
  "branch": "main",
  "app_name": "webapp",
  "environment": "dev",
  "run_type": "plan-apply",
  "workspace_id": "ws-abc123",
  "tfe_run_id": "run-xyz789",
  "message": "Terraform plan in progress",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:31:00Z"
}
```

### 4.3 POST /callback

Handles callbacks from TFE for run status updates.

**Request:**
```json
{
  "run_id": "run-xyz789",
  "run_status": "applied",
  "message": "Apply complete",
  "workflow_id": "deployment-550e8400"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Callback processed"
}
```

**Flow:**
1. Validate callback payload
2. Find deployment by TFE run ID
3. Signal Temporal workflow with status update
4. Update deployment status in database

### 4.4 GET /poll/:id

Polls the current status of a deployment (lightweight status check).

**Response:**
```json
{
  "deployment_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "tfe_run_id": "run-xyz789",
  "tfe_run_status": "planning",
  "message": "Terraform plan in progress"
}
```

---

## 5. Temporal Workflows

### 5.1 DeploymentWorkflow

The main orchestration workflow that coordinates the entire deployment process.

```
┌─────────────────────────────────────────────────────────────────────┐
│                      DeploymentWorkflow                              │
│                                                                      │
│  Input: DeploymentWorkflowInput                                      │
│  ├── deployment_id                                                   │
│  ├── git_repo                                                        │
│  ├── branch                                                          │
│  ├── app_name                                                        │
│  ├── environment                                                     │
│  ├── run_type                                                        │
│  └── registry_config (from lookup)                                   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Step 1: Update Status to "running"                           │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Step 2: Execute WorkspaceActivity                            │    │
│  │ - Check if workspace exists                                   │    │
│  │ - Create workspace if missing                                 │    │
│  │ - Configure workspace settings                                │    │
│  │ - Return workspace_id                                         │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Step 3: Execute TFRunActivity                                │    │
│  │ - Create TFE run with run_type                               │    │
│  │ - Submit configuration version                                │    │
│  │ - Return tfe_run_id                                          │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Step 4: Poll/Wait for Run Completion                         │    │
│  │ - Poll TFE run status periodically                           │    │
│  │ - OR wait for callback signal                                │    │
│  │ - Handle timeouts                                             │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Step 5: Update Final Status                                  │    │
│  │ - Set status to "success" or "failed"                        │    │
│  │ - Store result message                                        │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  Output: DeploymentWorkflowOutput                                    │
│  ├── success (bool)                                                  │
│  ├── status                                                          │
│  └── message                                                         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 WorkspaceActivity

Manages TFE workspace creation and configuration.

**Input:**
```go
type WorkspaceActivityInput struct {
    DeploymentID    string
    AppName         string
    Environment     string
    Organization    string
    WorkspacePrefix string
    TerraformVersion string
    WorkingDirectory string
    VCSRepo         string
    GitRepo         string
    Branch          string
}
```

**Output:**
```go
type WorkspaceActivityOutput struct {
    WorkspaceID    string
    TFEWorkspaceID string
    WorkspaceName  string
    Created        bool
}
```

**Logic:**
1. Generate workspace name: `{workspace_prefix}-{app_name}-{environment}`
2. Check if workspace exists in TFE
3. If not exists, create new workspace with:
   - Terraform version
   - Working directory
   - VCS connection (optional)
   - Auto-apply setting
4. Configure workspace variables (Azure identity, subscription, etc.)
5. Return workspace details

### 5.3 TFRunActivity

Manages TFE run creation and status polling.

**Input:**
```go
type TFRunActivityInput struct {
    DeploymentID   string
    WorkspaceID    string
    TFEWorkspaceID string
    RunType        string
    GitRepo        string
    Branch         string
    Message        string
}
```

**Output:**
```go
type TFRunActivityOutput struct {
    TFERunID  string
    Status    string
    Message   string
}
```

**Logic:**
1. Create configuration version in TFE
2. Upload Terraform code from git repo/branch
3. Create run with appropriate type:
   - `plan`: Plan only, no apply
   - `plan-apply`: Plan and auto-apply
   - `plan-destroy`: Plan destruction
   - `destroy`: Immediate destroy
4. Return run ID for tracking

---

## 6. Deployment Flow

### 6.1 Complete Deployment Sequence

```
┌──────────┐     ┌───────────┐     ┌──────────┐     ┌──────────┐     ┌─────┐
│  Client  │     │    API    │     │ Temporal │     │ Activity │     │ TFE │
└────┬─────┘     └─────┬─────┘     └────┬─────┘     └────┬─────┘     └──┬──┘
     │                 │                │                │              │
     │ POST /deploy    │                │                │              │
     │────────────────▶│                │                │              │
     │                 │                │                │              │
     │                 │ Lookup Registry│                │              │
     │                 │───────┐        │                │              │
     │                 │       │        │                │              │
     │                 │◀──────┘        │                │              │
     │                 │                │                │              │
     │                 │ Create Deployment               │              │
     │                 │───────┐        │                │              │
     │                 │       │        │                │              │
     │                 │◀──────┘        │                │              │
     │                 │                │                │              │
     │                 │ Start Workflow │                │              │
     │                 │───────────────▶│                │              │
     │                 │                │                │              │
     │ {deployment_id} │                │                │              │
     │◀────────────────│                │                │              │
     │                 │                │                │              │
     │                 │                │ WorkspaceActivity              │
     │                 │                │───────────────▶│              │
     │                 │                │                │              │
     │                 │                │                │ Check/Create │
     │                 │                │                │─────────────▶│
     │                 │                │                │              │
     │                 │                │                │ workspace_id │
     │                 │                │                │◀─────────────│
     │                 │                │                │              │
     │                 │                │ workspace_id   │              │
     │                 │                │◀───────────────│              │
     │                 │                │                │              │
     │                 │                │ TFRunActivity  │              │
     │                 │                │───────────────▶│              │
     │                 │                │                │              │
     │                 │                │                │ Create Run   │
     │                 │                │                │─────────────▶│
     │                 │                │                │              │
     │                 │                │                │ run_id       │
     │                 │                │                │◀─────────────│
     │                 │                │                │              │
     │                 │                │ run_id         │              │
     │                 │                │◀───────────────│              │
     │                 │                │                │              │
     │                 │                │ Poll Status    │              │
     │                 │                │───────────────▶│              │
     │                 │                │                │              │
     │                 │                │                │ Get Status   │
     │                 │                │                │─────────────▶│
     │                 │                │                │              │
     │                 │                │                │ completed    │
     │                 │                │                │◀─────────────│
     │                 │                │                │              │
     │                 │                │ final_status   │              │
     │                 │                │◀───────────────│              │
     │                 │                │                │              │
     │                 │                │ Update DB      │              │
     │                 │◀───────────────│                │              │
     │                 │                │                │              │
     │ GET /status/:id │                │                │              │
     │────────────────▶│                │                │              │
     │                 │                │                │              │
     │ {status: success}               │                │              │
     │◀────────────────│                │                │              │
     │                 │                │                │              │
```

### 6.2 Run Type Handling

| Run Type | TFE Behavior |
|----------|--------------|
| plan | Creates a speculative plan, no apply |
| plan-apply | Creates plan, auto-applies if successful |
| plan-destroy | Creates destroy plan, waits for confirmation |
| destroy | Immediately queues destroy operation |

### 6.3 Run Hooks Flow

The service supports lifecycle hooks that execute at specific points during a TFE run:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TFE Run Lifecycle with Hooks                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐                                                           │
│  │  Run Created │                                                           │
│  └──────┬───────┘                                                           │
│         │                                                                    │
│         ▼                                                                    │
│  ┌──────────────┐                                                           │
│  │   Planning   │                                                           │
│  └──────┬───────┘                                                           │
│         │                                                                    │
│         ▼                                                                    │
│  ┌──────────────┐     ┌─────────────────────────────────────┐              │
│  │ Plan Complete│────▶│  POST-PLAN HOOK                     │              │
│  └──────┬───────┘     │  - Notify stakeholders              │              │
│         │             │  - Run cost estimation              │              │
│         │             │  - Validate plan output             │              │
│         │             │  - Send to approval system          │              │
│         │             └─────────────────────────────────────┘              │
│         ▼                                                                    │
│  ┌──────────────┐     ┌─────────────────────────────────────┐              │
│  │ Pre-Apply    │────▶│  PRE-APPLY HOOK                     │              │
│  │ (Confirmed)  │     │  - Final validation checks          │              │
│  └──────┬───────┘     │  - Create backup/snapshot           │              │
│         │             │  - Notify on-call team              │              │
│         │             │  - Lock related resources           │              │
│         │             └─────────────────────────────────────┘              │
│         ▼                                                                    │
│  ┌──────────────┐                                                           │
│  │   Applying   │                                                           │
│  └──────┬───────┘                                                           │
│         │                                                                    │
│         ▼                                                                    │
│  ┌──────────────┐     ┌─────────────────────────────────────┐              │
│  │Apply Complete│────▶│  POST-APPLY HOOK                    │              │
│  └──────┬───────┘     │  - Update CMDB/inventory            │              │
│         │             │  - Trigger downstream deployments   │              │
│         │             │  - Run smoke tests                  │              │
│         │             │  - Send completion notification     │              │
│         │             │  - Update monitoring dashboards     │              │
│         │             └─────────────────────────────────────┘              │
│         ▼                                                                    │
│  ┌──────────────┐                                                           │
│  │  Run Complete│                                                           │
│  └──────────────┘                                                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Hook Execution Details:**

| Hook | Trigger Point | Common Use Cases |
|------|--------------|------------------|
| post-plan | After plan completes successfully | Cost estimation, plan review, approval request |
| pre-apply | After approval, before apply starts | Final checks, create snapshots, notify teams |
| post-apply | After apply completes (success or failure) | Update inventory, trigger tests, notifications |

**Hook Failure Handling:**

- If `fail_on_error` is true and hook fails, the deployment is marked as failed
- If `fail_on_error` is false, hook failure is logged but deployment continues
- Hooks have configurable timeouts (default: 30 seconds)
- Failed hooks can be retried based on configuration

---

## 7. Internal Registry

### 7.1 Registry Overview

The Internal Registry (AppRegistry) is a central configuration store that maps application + environment combinations to their deployment configurations. This enables:

- **Application Onboarding**: Teams register their applications once with all environment-specific configurations
- **Consistent Deployments**: Same application code deploys with correct settings per environment
- **Azure Integration**: Automatic injection of Azure credentials and resource configurations
- **TFE Configuration**: Pre-defined workspace settings, run hooks, and variables

### 7.2 Registry Structure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Internal Registry                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Lookup Key: app_name + environment                                          │
│  Example: "webapp" + "prod" → Registry Entry                                │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                        Registry Entry                               │     │
│  │                                                                      │     │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐  │     │
│  │  │ Identity Config  │  │ Workspace Config │  │ Run Hooks       │  │     │
│  │  │ - azure_identity │  │ - organization   │  │ - post_plan     │  │     │
│  │  │ - subscription_id│  │ - workspace_prefix│ │ - pre_apply     │  │     │
│  │  │ - tenant_id      │  │ - tf_version     │  │ - post_apply    │  │     │
│  │  │ - resource_group │  │ - working_dir    │  │                 │  │     │
│  │  └──────────────────┘  │ - auto_apply     │  └─────────────────┘  │     │
│  │                         │ - execution_mode │                       │     │
│  │  ┌──────────────────┐  └──────────────────┘  ┌─────────────────┐  │     │
│  │  │ Variables        │                         │ Notifications   │  │     │
│  │  │ - TF_VAR_*       │                         │ - slack_url     │  │     │
│  │  │ - env vars       │                         │ - teams_url     │  │     │
│  │  │ - secrets refs   │                         │ - email         │  │     │
│  │  └──────────────────┘                         └─────────────────┘  │     │
│  │                                                                      │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Registry Entry Example (JSON)

```json
{
  "id": "reg-550e8400-e29b-41d4-a716-446655440000",
  "app_name": "webapp",
  "environment": "prod",
  "azure_identity": "webapp-prod-managed-identity",
  "subscription_id": "12345678-1234-1234-1234-123456789012",
  "tenant_id": "87654321-4321-4321-4321-210987654321",
  "resource_group": "rg-webapp-prod-eastus",
  "workspace_config": {
    "organization": "acme-corp",
    "workspace_prefix": "webapp",
    "terraform_version": "1.6.0",
    "working_directory": "terraform/environments/prod",
    "auto_apply": false,
    "execution_mode": "remote",
    "agent_pool_id": null,
    "vcs_repo": "acme-corp/webapp-infrastructure"
  },
  "run_hooks": {
    "post_plan": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/deployments/plan-review",
      "webhook_method": "POST",
      "webhook_headers": {
        "Authorization": "Bearer ${WEBHOOK_TOKEN}",
        "Content-Type": "application/json"
      },
      "webhook_payload_template": "{\"deployment_id\": \"{{.DeploymentID}}\", \"plan_url\": \"{{.PlanURL}}\", \"changes\": {{.ChangeSummary}}}",
      "timeout_seconds": 30,
      "fail_on_error": false
    },
    "pre_apply": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/deployments/pre-apply-check",
      "webhook_method": "POST",
      "webhook_headers": {
        "Authorization": "Bearer ${WEBHOOK_TOKEN}"
      },
      "timeout_seconds": 60,
      "fail_on_error": true
    },
    "post_apply": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/deployments/complete",
      "webhook_method": "POST",
      "script_path": "scripts/post-deploy-validation.sh",
      "timeout_seconds": 120,
      "fail_on_error": false
    }
  },
  "variables": {
    "ARM_CLIENT_ID": "${azure_identity}",
    "ARM_SUBSCRIPTION_ID": "${subscription_id}",
    "ARM_TENANT_ID": "${tenant_id}",
    "TF_VAR_environment": "prod",
    "TF_VAR_resource_group": "${resource_group}",
    "TF_VAR_location": "eastus",
    "TF_VAR_app_name": "webapp",
    "TF_VAR_sku_tier": "Premium",
    "TF_VAR_instance_count": "3"
  },
  "tags": {
    "Application": "webapp",
    "Environment": "prod",
    "CostCenter": "CC-12345",
    "Owner": "platform-team@acme.com",
    "ManagedBy": "terraform"
  },
  "notification_config": {
    "slack_webhook_url": "https://hooks.slack.com/services/REPLACE_WITH_ACTUAL_WEBHOOK",
    "teams_webhook_url": null,
    "email_recipients": ["platform-team@acme.com", "webapp-owners@acme.com"],
    "notify_on_success": true,
    "notify_on_failure": true,
    "notify_on_pending_approval": true
  },
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

### 7.4 Environment-Specific Configuration Examples

#### Development Environment

```json
{
  "app_name": "webapp",
  "environment": "dev",
  "azure_identity": "webapp-dev-identity",
  "subscription_id": "dev-subscription-id",
  "workspace_config": {
    "organization": "acme-corp",
    "workspace_prefix": "webapp-dev",
    "terraform_version": "1.6.0",
    "working_directory": "terraform/environments/dev",
    "auto_apply": true,
    "execution_mode": "remote"
  },
  "run_hooks": {
    "post_plan": { "enabled": false },
    "pre_apply": { "enabled": false },
    "post_apply": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/dev/smoke-tests",
      "fail_on_error": false
    }
  },
  "variables": {
    "TF_VAR_sku_tier": "Basic",
    "TF_VAR_instance_count": "1"
  }
}
```

#### Staging Environment

```json
{
  "app_name": "webapp",
  "environment": "staging",
  "azure_identity": "webapp-staging-identity",
  "subscription_id": "staging-subscription-id",
  "workspace_config": {
    "organization": "acme-corp",
    "workspace_prefix": "webapp-staging",
    "terraform_version": "1.6.0",
    "working_directory": "terraform/environments/staging",
    "auto_apply": false,
    "execution_mode": "remote"
  },
  "run_hooks": {
    "post_plan": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/staging/plan-review"
    },
    "pre_apply": { "enabled": false },
    "post_apply": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/staging/integration-tests",
      "fail_on_error": true
    }
  },
  "variables": {
    "TF_VAR_sku_tier": "Standard",
    "TF_VAR_instance_count": "2"
  }
}
```

#### Production Environment

```json
{
  "app_name": "webapp",
  "environment": "prod",
  "azure_identity": "webapp-prod-identity",
  "subscription_id": "prod-subscription-id",
  "workspace_config": {
    "organization": "acme-corp",
    "workspace_prefix": "webapp-prod",
    "terraform_version": "1.6.0",
    "working_directory": "terraform/environments/prod",
    "auto_apply": false,
    "execution_mode": "agent",
    "agent_pool_id": "apool-prod-secure"
  },
  "run_hooks": {
    "post_plan": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/prod/approval-request",
      "fail_on_error": true
    },
    "pre_apply": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/prod/change-window-check",
      "fail_on_error": true
    },
    "post_apply": {
      "enabled": true,
      "webhook_url": "https://api.internal.acme.com/prod/cmdb-update",
      "script_path": "scripts/prod-validation.sh",
      "fail_on_error": false
    }
  },
  "variables": {
    "TF_VAR_sku_tier": "Premium",
    "TF_VAR_instance_count": "3"
  },
  "notification_config": {
    "notify_on_pending_approval": true
  }
}
```

### 7.5 Registry Lookup Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Registry Lookup Process                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Deploy Request                                                             │
│   ┌──────────────────────────────┐                                          │
│   │ app_name: "webapp"           │                                          │
│   │ environment: "prod"          │                                          │
│   │ git_repo: "github.com/..."   │                                          │
│   │ branch: "release/v2.0"       │                                          │
│   └──────────────┬───────────────┘                                          │
│                  │                                                           │
│                  ▼                                                           │
│   ┌──────────────────────────────┐                                          │
│   │    Registry Lookup           │                                          │
│   │    Key: "webapp:prod"        │                                          │
│   └──────────────┬───────────────┘                                          │
│                  │                                                           │
│          ┌───────┴───────┐                                                  │
│          │               │                                                   │
│          ▼               ▼                                                   │
│   ┌─────────────┐ ┌─────────────┐                                          │
│   │   Found     │ │  Not Found  │                                          │
│   └──────┬──────┘ └──────┬──────┘                                          │
│          │               │                                                   │
│          ▼               ▼                                                   │
│   ┌─────────────┐ ┌─────────────────────┐                                   │
│   │ Return      │ │ Return Error:       │                                   │
│   │ Registry    │ │ "Application not    │                                   │
│   │ Config      │ │  registered for     │                                   │
│   └─────────────┘ │  environment"       │                                   │
│                   └─────────────────────┘                                   │
│                                                                              │
│   Merged Configuration for Workflow:                                         │
│   ┌──────────────────────────────────────────────────────────────────┐     │
│   │ {                                                                  │     │
│   │   deployment_id: "uuid",                                          │     │
│   │   git_repo: "github.com/...",    // from request                  │     │
│   │   branch: "release/v2.0",        // from request                  │     │
│   │   app_name: "webapp",            // from request                  │     │
│   │   environment: "prod",           // from request                  │     │
│   │   azure_identity: "...",         // from registry                 │     │
│   │   subscription_id: "...",        // from registry                 │     │
│   │   workspace_config: {...},       // from registry                 │     │
│   │   run_hooks: {...},              // from registry                 │     │
│   │   variables: {...}               // from registry                 │     │
│   │ }                                                                  │     │
│   └──────────────────────────────────────────────────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.6 Onboarding a New Application

To deploy an application using this service, teams must first onboard by creating registry entries:

1. **Request onboarding** through internal portal or API
2. **Provide application details**:
   - Application name (unique identifier)
   - Git repository URL
   - Terraform working directory
3. **Configure per-environment settings**:
   - Azure subscription and identity
   - TFE workspace preferences
   - Run hooks (if needed)
   - Variables and secrets
4. **Registry entry created** by platform team
5. **Application ready** for deployments

---

## 8. Configuration

### 8.1 Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SERVER_PORT` | HTTP server port | 8080 |
| `TEMPORAL_HOST` | Temporal server address | localhost:7233 |
| `TEMPORAL_NAMESPACE` | Temporal namespace | default |
| `TFE_ADDRESS` | TFE API address | https://app.terraform.io |
| `TFE_TOKEN` | TFE API token | - |
| `TFE_ORGANIZATION` | Default TFE organization | - |

### 8.2 Configuration File (config.yaml)

```yaml
server:
  port: 8080
  read_timeout: 30s
  write_timeout: 30s

temporal:
  host: localhost:7233
  namespace: default
  task_queue: tfe-deployment-queue

tfe:
  address: https://app.terraform.io
  organization: my-org
  
logging:
  level: info
  format: json
```

---

## 9. Error Handling

### 9.1 Error Categories

| Category | HTTP Code | Description |
|----------|-----------|-------------|
| Validation Error | 400 | Invalid request parameters |
| Not Found | 404 | Deployment/workspace not found |
| Registry Not Found | 404 | App not registered for environment |
| TFE Error | 502 | TFE API failure |
| Workflow Error | 500 | Temporal workflow failure |
| Internal Error | 500 | Unexpected server error |

### 9.2 Retry Strategy

- **Temporal Activities**: Automatic retry with exponential backoff
- **TFE API Calls**: 3 retries with 5s, 10s, 20s delays
- **Status Polling**: Every 30 seconds, timeout after 1 hour

---

## 10. Project Structure

```
tfe-deployment-service/
├── api/
│   ├── gen/                    # Generated Go code from proto
│   │   └── deployment/
│   │       └── v1/
│   │           └── deployment.pb.go
│   └── proto/                  # Protocol buffer definitions
│       └── deployment/
│           └── v1/
│               └── deployment.proto
├── cmd/
│   └── service/
│       └── main.go             # Application entry point
├── docs/
│   └── design.md               # This design document
├── internal/
│   └── config/
│       └── config.go           # Configuration management
├── pkg/
│   ├── activities/             # Temporal activities
│   │   ├── workspace.go
│   │   └── tfrun.go
│   ├── db/                     # In-memory database
│   │   └── db.go
│   ├── handlers/               # HTTP handlers
│   │   └── handlers.go
│   ├── tfe/                    # TFE client
│   │   └── client.go
│   └── workflows/              # Temporal workflows
│       └── deployment.go
├── go.mod
├── go.sum
└── README.md
```

---

## 11. Security Considerations

### 11.1 Authentication & Authorization

- TFE API token stored securely (environment variable or secrets manager)
- API endpoints should be protected with authentication (future enhancement)
- Service-to-service communication via mTLS (future enhancement)

### 11.2 Data Protection

- Sensitive data (tokens, credentials) never logged
- In-memory database cleared on restart (no persistence of sensitive data)
- TFE workspace variables marked as sensitive where appropriate

### 11.3 Input Validation

- All API inputs validated against proto schema
- Git repository URLs validated for format
- Environment names restricted to allowed values

---

## 12. Monitoring & Observability

### 12.1 Logging

- Structured JSON logging
- Request/response logging with correlation IDs
- Workflow execution logging via Temporal

### 12.2 Metrics (Future Enhancement)

- Deployment success/failure rates
- Average deployment duration
- TFE API latency
- Workflow queue depth

### 12.3 Health Checks

- `GET /health` - Service health
- `GET /ready` - Readiness probe (Temporal connection)

---

## 13. Future Enhancements

1. **Persistent Storage**: Replace in-memory DB with PostgreSQL
2. **Authentication**: Add OAuth2/JWT authentication
3. **RBAC**: Role-based access control for deployments
4. **Notifications**: Slack/Teams notifications on completion
5. **Approval Workflows**: Manual approval gates for production
6. **Cost Estimation**: Integrate TFE cost estimation
7. **Drift Detection**: Scheduled drift detection runs
8. **Multi-tenancy**: Support multiple organizations
