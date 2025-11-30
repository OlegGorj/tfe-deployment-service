package db

import (
	"errors"
	"sync"
	"time"

	pb "github.com/OlegGorj/tfe-deployment-service/api/gen/deployment/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
)

// InMemoryDB provides an in-memory database for the service
type InMemoryDB struct {
	deployments map[string]*pb.Deployment
	workspaces  map[string]*pb.Workspace
	appRegistry map[string]*pb.AppRegistry
	mu          sync.RWMutex
}

// NewInMemoryDB creates a new in-memory database instance
func NewInMemoryDB() *InMemoryDB {
	return &InMemoryDB{
		deployments: make(map[string]*pb.Deployment),
		workspaces:  make(map[string]*pb.Workspace),
		appRegistry: make(map[string]*pb.AppRegistry),
	}
}

// Deployment operations

// CreateDeployment creates a new deployment record
func (db *InMemoryDB) CreateDeployment(d *pb.Deployment) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if d.Id == "" {
		d.Id = uuid.New().String()
	}
	if _, exists := db.deployments[d.Id]; exists {
		return ErrAlreadyExists
	}

	now := timestamppb.New(time.Now())
	d.CreatedAt = now
	d.UpdatedAt = now
	d.Status = pb.DeploymentStatus_DEPLOYMENT_STATUS_PENDING

	db.deployments[d.Id] = d
	return nil
}

// GetDeployment retrieves a deployment by ID
func (db *InMemoryDB) GetDeployment(id string) (*pb.Deployment, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	d, exists := db.deployments[id]
	if !exists {
		return nil, ErrNotFound
	}
	return d, nil
}

// UpdateDeployment updates an existing deployment
func (db *InMemoryDB) UpdateDeployment(d *pb.Deployment) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.deployments[d.Id]; !exists {
		return ErrNotFound
	}

	d.UpdatedAt = timestamppb.New(time.Now())
	db.deployments[d.Id] = d
	return nil
}

// UpdateDeploymentStatus updates only the status and message of a deployment
func (db *InMemoryDB) UpdateDeploymentStatus(id string, status pb.DeploymentStatus, message string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	d, exists := db.deployments[id]
	if !exists {
		return ErrNotFound
	}

	d.Status = status
	d.Message = message
	d.UpdatedAt = timestamppb.New(time.Now())
	return nil
}

// UpdateDeploymentTFERunID updates the TFE run ID for a deployment
func (db *InMemoryDB) UpdateDeploymentTFERunID(id string, tfeRunID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	d, exists := db.deployments[id]
	if !exists {
		return ErrNotFound
	}

	d.TfeRunId = tfeRunID
	d.UpdatedAt = timestamppb.New(time.Now())
	return nil
}

// UpdateDeploymentWorkspaceID updates the workspace ID for a deployment
func (db *InMemoryDB) UpdateDeploymentWorkspaceID(id string, workspaceID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	d, exists := db.deployments[id]
	if !exists {
		return ErrNotFound
	}

	d.WorkspaceId = workspaceID
	d.UpdatedAt = timestamppb.New(time.Now())
	return nil
}

// ListDeployments returns all deployments
func (db *InMemoryDB) ListDeployments() []*pb.Deployment {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := make([]*pb.Deployment, 0, len(db.deployments))
	for _, d := range db.deployments {
		result = append(result, d)
	}
	return result
}

// Workspace operations

// CreateWorkspace creates a new workspace record
func (db *InMemoryDB) CreateWorkspace(w *pb.Workspace) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if w.Id == "" {
		w.Id = uuid.New().String()
	}
	if _, exists := db.workspaces[w.Id]; exists {
		return ErrAlreadyExists
	}

	now := timestamppb.New(time.Now())
	w.CreatedAt = now
	w.UpdatedAt = now

	db.workspaces[w.Id] = w
	return nil
}

// GetWorkspace retrieves a workspace by ID
func (db *InMemoryDB) GetWorkspace(id string) (*pb.Workspace, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	w, exists := db.workspaces[id]
	if !exists {
		return nil, ErrNotFound
	}
	return w, nil
}

// GetWorkspaceByName retrieves a workspace by name
func (db *InMemoryDB) GetWorkspaceByName(name string) (*pb.Workspace, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, w := range db.workspaces {
		if w.Name == name {
			return w, nil
		}
	}
	return nil, ErrNotFound
}

// GetWorkspaceByAppEnv retrieves a workspace by app name and environment
func (db *InMemoryDB) GetWorkspaceByAppEnv(appName, environment string) (*pb.Workspace, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, w := range db.workspaces {
		if w.AppName == appName && w.Environment == environment {
			return w, nil
		}
	}
	return nil, ErrNotFound
}

// UpdateWorkspace updates an existing workspace
func (db *InMemoryDB) UpdateWorkspace(w *pb.Workspace) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.workspaces[w.Id]; !exists {
		return ErrNotFound
	}

	w.UpdatedAt = timestamppb.New(time.Now())
	db.workspaces[w.Id] = w
	return nil
}

// ListWorkspaces returns all workspaces
func (db *InMemoryDB) ListWorkspaces() []*pb.Workspace {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := make([]*pb.Workspace, 0, len(db.workspaces))
	for _, w := range db.workspaces {
		result = append(result, w)
	}
	return result
}

// AppRegistry operations

// CreateAppRegistry creates a new app registry record
func (db *InMemoryDB) CreateAppRegistry(a *pb.AppRegistry) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if a.Id == "" {
		a.Id = uuid.New().String()
	}

	// Use app_name + environment as unique key
	key := a.AppName + ":" + a.Environment
	if _, exists := db.appRegistry[key]; exists {
		return ErrAlreadyExists
	}

	now := timestamppb.New(time.Now())
	a.CreatedAt = now
	a.UpdatedAt = now

	db.appRegistry[key] = a
	return nil
}

// GetAppRegistry retrieves an app registry by app name and environment
func (db *InMemoryDB) GetAppRegistry(appName, environment string) (*pb.AppRegistry, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	key := appName + ":" + environment
	a, exists := db.appRegistry[key]
	if !exists {
		return nil, ErrNotFound
	}
	return a, nil
}

// UpdateAppRegistry updates an existing app registry
func (db *InMemoryDB) UpdateAppRegistry(a *pb.AppRegistry) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	key := a.AppName + ":" + a.Environment
	if _, exists := db.appRegistry[key]; !exists {
		return ErrNotFound
	}

	a.UpdatedAt = timestamppb.New(time.Now())
	db.appRegistry[key] = a
	return nil
}

// ListAppRegistry returns all app registry entries
func (db *InMemoryDB) ListAppRegistry() []*pb.AppRegistry {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := make([]*pb.AppRegistry, 0, len(db.appRegistry))
	for _, a := range db.appRegistry {
		result = append(result, a)
	}
	return result
}

// SeedAppRegistry adds sample app registry data for testing
func (db *InMemoryDB) SeedAppRegistry() {
	sampleApps := []*pb.AppRegistry{
		{
			AppName:        "webapp",
			Environment:    "dev",
			AzureIdentity:  "webapp-dev-identity",
			SubscriptionId: "00000000-0000-0000-0000-000000000001",
			TenantId:       "00000000-0000-0000-0000-000000000010",
			ResourceGroup:  "rg-webapp-dev",
			WorkspaceConfig: &pb.WorkspaceConfig{
				Organization:     "my-org",
				WorkspacePrefix:  "webapp-dev",
				TerraformVersion: "1.6.0",
				WorkingDirectory: "terraform",
				AutoApply:        true,
				ExecutionMode:    "remote",
			},
			RunHooks: &pb.RunHooks{
				PostPlan:  &pb.HookConfig{Enabled: false},
				PreApply:  &pb.HookConfig{Enabled: false},
				PostApply: &pb.HookConfig{Enabled: true, WebhookUrl: "https://api.internal/dev/smoke-tests", FailOnError: false},
			},
			Variables: map[string]string{
				"TF_VAR_environment": "dev",
				"TF_VAR_sku_tier":    "Basic",
			},
			Tags: map[string]string{
				"Environment": "dev",
				"ManagedBy":   "terraform",
			},
		},
		{
			AppName:        "webapp",
			Environment:    "staging",
			AzureIdentity:  "webapp-staging-identity",
			SubscriptionId: "00000000-0000-0000-0000-000000000002",
			TenantId:       "00000000-0000-0000-0000-000000000010",
			ResourceGroup:  "rg-webapp-staging",
			WorkspaceConfig: &pb.WorkspaceConfig{
				Organization:     "my-org",
				WorkspacePrefix:  "webapp-staging",
				TerraformVersion: "1.6.0",
				WorkingDirectory: "terraform",
				AutoApply:        false,
				ExecutionMode:    "remote",
			},
			RunHooks: &pb.RunHooks{
				PostPlan:  &pb.HookConfig{Enabled: true, WebhookUrl: "https://api.internal/staging/plan-review"},
				PreApply:  &pb.HookConfig{Enabled: false},
				PostApply: &pb.HookConfig{Enabled: true, WebhookUrl: "https://api.internal/staging/integration-tests", FailOnError: true},
			},
			Variables: map[string]string{
				"TF_VAR_environment": "staging",
				"TF_VAR_sku_tier":    "Standard",
			},
			Tags: map[string]string{
				"Environment": "staging",
				"ManagedBy":   "terraform",
			},
		},
		{
			AppName:        "webapp",
			Environment:    "prod",
			AzureIdentity:  "webapp-prod-identity",
			SubscriptionId: "00000000-0000-0000-0000-000000000003",
			TenantId:       "00000000-0000-0000-0000-000000000010",
			ResourceGroup:  "rg-webapp-prod",
			WorkspaceConfig: &pb.WorkspaceConfig{
				Organization:     "my-org",
				WorkspacePrefix:  "webapp-prod",
				TerraformVersion: "1.6.0",
				WorkingDirectory: "terraform",
				AutoApply:        false,
				ExecutionMode:    "agent",
				AgentPoolId:      "apool-prod-secure",
			},
			RunHooks: &pb.RunHooks{
				PostPlan:  &pb.HookConfig{Enabled: true, WebhookUrl: "https://api.internal/prod/approval-request", FailOnError: true},
				PreApply:  &pb.HookConfig{Enabled: true, WebhookUrl: "https://api.internal/prod/change-window-check", FailOnError: true},
				PostApply: &pb.HookConfig{Enabled: true, WebhookUrl: "https://api.internal/prod/cmdb-update", ScriptPath: "scripts/prod-validation.sh", FailOnError: false},
			},
			Variables: map[string]string{
				"TF_VAR_environment": "prod",
				"TF_VAR_sku_tier":    "Premium",
			},
			Tags: map[string]string{
				"Environment": "prod",
				"ManagedBy":   "terraform",
			},
			NotificationConfig: &pb.NotificationConfig{
				SlackWebhookUrl:         "https://hooks.slack.com/services/PLACEHOLDER",
				NotifyOnSuccess:         true,
				NotifyOnFailure:         true,
				NotifyOnPendingApproval: true,
			},
		},
		{
			AppName:        "api-service",
			Environment:    "dev",
			AzureIdentity:  "api-service-dev-identity",
			SubscriptionId: "00000000-0000-0000-0000-000000000004",
			TenantId:       "00000000-0000-0000-0000-000000000010",
			ResourceGroup:  "rg-api-service-dev",
			WorkspaceConfig: &pb.WorkspaceConfig{
				Organization:     "my-org",
				WorkspacePrefix:  "api-service-dev",
				TerraformVersion: "1.6.0",
				WorkingDirectory: "infra",
				AutoApply:        true,
				ExecutionMode:    "remote",
			},
			RunHooks: &pb.RunHooks{
				PostPlan:  &pb.HookConfig{Enabled: false},
				PreApply:  &pb.HookConfig{Enabled: false},
				PostApply: &pb.HookConfig{Enabled: false},
			},
			Variables: map[string]string{
				"TF_VAR_environment": "dev",
			},
			Tags: map[string]string{
				"Environment": "dev",
				"ManagedBy":   "terraform",
			},
		},
	}

	for _, app := range sampleApps {
		_ = db.CreateAppRegistry(app)
	}
}
