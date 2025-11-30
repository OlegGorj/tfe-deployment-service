package activities

import (
	"context"
	"fmt"

	pb "github.com/OlegGorj/tfe-deployment-service/api/gen/deployment/v1"
	"github.com/OlegGorj/tfe-deployment-service/pkg/db"
	"github.com/OlegGorj/tfe-deployment-service/pkg/tfe"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Activities holds all Temporal activities and their dependencies
type Activities struct {
	DB        *db.InMemoryDB
	TFEClient *tfe.Client
}

// NewActivities creates a new Activities instance
func NewActivities(database *db.InMemoryDB, tfeClient *tfe.Client) *Activities {
	return &Activities{
		DB:        database,
		TFEClient: tfeClient,
	}
}

// WorkspaceActivityInput contains input for the workspace activity
type WorkspaceActivityInput struct {
	DeploymentID     string
	AppName          string
	Environment      string
	Organization     string
	WorkspacePrefix  string
	TerraformVersion string
	WorkingDirectory string
	ExecutionMode    string
	AgentPoolID      string
	VCSRepo          string
	GitRepo          string
	Branch           string
	Variables        map[string]string
}

// WorkspaceActivityOutput contains output from the workspace activity
type WorkspaceActivityOutput struct {
	WorkspaceID    string
	TFEWorkspaceID string
	WorkspaceName  string
	Created        bool
}

// WorkspaceActivity creates or retrieves a TFE workspace
func (a *Activities) WorkspaceActivity(ctx context.Context, input WorkspaceActivityInput) (*WorkspaceActivityOutput, error) {
	// Generate workspace name
	workspaceName := fmt.Sprintf("%s-%s-%s", input.WorkspacePrefix, input.AppName, input.Environment)

	// Check if workspace already exists in our DB
	existingWorkspace, err := a.DB.GetWorkspaceByAppEnv(input.AppName, input.Environment)
	if err == nil && existingWorkspace != nil {
		return &WorkspaceActivityOutput{
			WorkspaceID:    existingWorkspace.Id,
			TFEWorkspaceID: existingWorkspace.TfeWorkspaceId,
			WorkspaceName:  existingWorkspace.Name,
			Created:        false,
		}, nil
	}

	// Create workspace in TFE
	tfeOutput, err := a.TFEClient.GetOrCreateWorkspace(ctx, &tfe.WorkspaceCreateInput{
		Name:             workspaceName,
		Organization:     input.Organization,
		TerraformVersion: input.TerraformVersion,
		WorkingDirectory: input.WorkingDirectory,
		AutoApply:        false, // We control apply through run type
		ExecutionMode:    input.ExecutionMode,
		AgentPoolID:      input.AgentPoolID,
		VCSRepo:          input.VCSRepo,
		GitRepo:          input.GitRepo,
		Branch:           input.Branch,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create TFE workspace: %w", err)
	}

	// Configure workspace variables
	if len(input.Variables) > 0 {
		if err := a.TFEClient.ConfigureWorkspaceVariables(ctx, tfeOutput.TFEWorkspaceID, input.Variables); err != nil {
			return nil, fmt.Errorf("failed to configure workspace variables: %w", err)
		}
	}

	// Store workspace in our DB
	workspace := &pb.Workspace{
		Name:           workspaceName,
		TfeWorkspaceId: tfeOutput.TFEWorkspaceID,
		Organization:   input.Organization,
		AppName:        input.AppName,
		Environment:    input.Environment,
	}
	if err := a.DB.CreateWorkspace(workspace); err != nil {
		// Log but don't fail - workspace was created in TFE
		fmt.Printf("Warning: failed to store workspace in DB: %v\n", err)
	}

	// Update deployment with workspace ID
	if err := a.DB.UpdateDeploymentWorkspaceID(input.DeploymentID, workspace.Id); err != nil {
		fmt.Printf("Warning: failed to update deployment workspace ID: %v\n", err)
	}

	return &WorkspaceActivityOutput{
		WorkspaceID:    workspace.Id,
		TFEWorkspaceID: tfeOutput.TFEWorkspaceID,
		WorkspaceName:  workspaceName,
		Created:        tfeOutput.Created,
	}, nil
}

// TFRunActivityInput contains input for the TF run activity
type TFRunActivityInput struct {
	DeploymentID   string
	WorkspaceID    string
	TFEWorkspaceID string
	RunType        pb.RunType
	GitRepo        string
	Branch         string
	Message        string
	AutoApply      bool
}

// TFRunActivityOutput contains output from the TF run activity
type TFRunActivityOutput struct {
	TFERunID string
	Status   string
	Message  string
}

// TFRunActivity creates a Terraform run in TFE
func (a *Activities) TFRunActivity(ctx context.Context, input TFRunActivityInput) (*TFRunActivityOutput, error) {
	// Determine run parameters based on run type
	isDestroy := false
	planOnly := false
	autoApply := input.AutoApply

	switch input.RunType {
	case pb.RunType_RUN_TYPE_PLAN:
		planOnly = true
		autoApply = false
	case pb.RunType_RUN_TYPE_PLAN_APPLY:
		// Default behavior
	case pb.RunType_RUN_TYPE_PLAN_DESTROY:
		isDestroy = true
		autoApply = false
	case pb.RunType_RUN_TYPE_DESTROY:
		isDestroy = true
		autoApply = true
	}

	// Create the run in TFE
	runOutput, err := a.TFEClient.CreateRun(ctx, &tfe.RunCreateInput{
		WorkspaceID: input.TFEWorkspaceID,
		Message:     input.Message,
		IsDestroy:   isDestroy,
		AutoApply:   autoApply,
		PlanOnly:    planOnly,
		GitRepo:     input.GitRepo,
		Branch:      input.Branch,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create TFE run: %w", err)
	}

	// Update deployment with TFE run ID
	if err := a.DB.UpdateDeploymentTFERunID(input.DeploymentID, runOutput.RunID); err != nil {
		fmt.Printf("Warning: failed to update deployment TFE run ID: %v\n", err)
	}

	return &TFRunActivityOutput{
		TFERunID: runOutput.RunID,
		Status:   runOutput.Status,
		Message:  runOutput.Message,
	}, nil
}

// PollRunStatusInput contains input for polling run status
type PollRunStatusInput struct {
	DeploymentID string
	TFERunID     string
}

// PollRunStatusOutput contains output from polling run status
type PollRunStatusOutput struct {
	Status     string
	IsTerminal bool
	IsSuccess  bool
	Message    string
	PlanURL    string
}

// PollRunStatusActivity polls the status of a TFE run
func (a *Activities) PollRunStatusActivity(ctx context.Context, input PollRunStatusInput) (*PollRunStatusOutput, error) {
	statusOutput, err := a.TFEClient.GetRunStatus(ctx, input.TFERunID)
	if err != nil {
		return nil, fmt.Errorf("failed to get run status: %w", err)
	}

	return &PollRunStatusOutput{
		Status:     string(statusOutput.Status),
		IsTerminal: tfe.IsTerminalStatus(statusOutput.Status),
		IsSuccess:  tfe.IsSuccessStatus(statusOutput.Status),
		Message:    statusOutput.Message,
		PlanURL:    statusOutput.PlanURL,
	}, nil
}

// UpdateDeploymentStatusInput contains input for updating deployment status
type UpdateDeploymentStatusInput struct {
	DeploymentID string
	Status       pb.DeploymentStatus
	Message      string
}

// UpdateDeploymentStatusActivity updates the deployment status in the database
func (a *Activities) UpdateDeploymentStatusActivity(ctx context.Context, input UpdateDeploymentStatusInput) error {
	return a.DB.UpdateDeploymentStatus(input.DeploymentID, input.Status, input.Message)
}

// ExecuteHookInput contains input for executing a run hook
type ExecuteHookInput struct {
	DeploymentID  string
	HookType      string // "post_plan", "pre_apply", "post_apply"
	HookConfig    *pb.HookConfig
	RunID         string
	RunStatus     string
	PlanURL       string
	ChangeSummary string
}

// ExecuteHookOutput contains output from hook execution
type ExecuteHookOutput struct {
	Success bool
	Message string
}

// ExecuteHookActivity executes a run lifecycle hook
func (a *Activities) ExecuteHookActivity(ctx context.Context, input ExecuteHookInput) (*ExecuteHookOutput, error) {
	if input.HookConfig == nil || !input.HookConfig.Enabled {
		return &ExecuteHookOutput{
			Success: true,
			Message: "Hook disabled, skipping",
		}, nil
	}

	result, err := a.TFEClient.ExecuteHook(ctx, &tfe.HookExecutionInput{
		HookConfig:    input.HookConfig,
		DeploymentID:  input.DeploymentID,
		RunID:         input.RunID,
		RunStatus:     input.RunStatus,
		PlanURL:       input.PlanURL,
		ChangeSummary: input.ChangeSummary,
	})
	if err != nil {
		if input.HookConfig.FailOnError {
			return nil, fmt.Errorf("hook execution failed: %w", err)
		}
		return &ExecuteHookOutput{
			Success: false,
			Message: fmt.Sprintf("Hook failed but continuing: %v", err),
		}, nil
	}

	return &ExecuteHookOutput{
		Success: result.Success,
		Message: result.Message,
	}, nil
}

// LookupRegistryInput contains input for registry lookup
type LookupRegistryInput struct {
	AppName     string
	Environment string
}

// LookupRegistryActivity looks up application configuration from the registry
func (a *Activities) LookupRegistryActivity(ctx context.Context, input LookupRegistryInput) (*pb.AppRegistry, error) {
	registry, err := a.DB.GetAppRegistry(input.AppName, input.Environment)
	if err != nil {
		return nil, fmt.Errorf("application '%s' not registered for environment '%s': %w", input.AppName, input.Environment, err)
	}
	return registry, nil
}

// CreateDeploymentInput contains input for creating a deployment record
type CreateDeploymentInput struct {
	GitRepo     string
	Branch      string
	AppName     string
	Environment string
	RunType     pb.RunType
}

// CreateDeploymentActivity creates a new deployment record in the database
func (a *Activities) CreateDeploymentActivity(ctx context.Context, input CreateDeploymentInput) (*pb.Deployment, error) {
	deployment := &pb.Deployment{
		GitRepo:     input.GitRepo,
		Branch:      input.Branch,
		AppName:     input.AppName,
		Environment: input.Environment,
		RunType:     input.RunType,
		Status:      pb.DeploymentStatus_DEPLOYMENT_STATUS_PENDING,
		CreatedAt:   timestamppb.Now(),
		UpdatedAt:   timestamppb.Now(),
	}

	if err := a.DB.CreateDeployment(deployment); err != nil {
		return nil, fmt.Errorf("failed to create deployment: %w", err)
	}

	return deployment, nil
}
