package tfe

import (
	"context"
	"fmt"
	"time"

	pb "github.com/OlegGorj/tfe-deployment-service/api/gen/deployment/v1"
)

// RunStatus represents the status of a TFE run
type RunStatus string

const (
	RunStatusPending            RunStatus = "pending"
	RunStatusPlanQueued         RunStatus = "plan_queued"
	RunStatusPlanning           RunStatus = "planning"
	RunStatusPlanned            RunStatus = "planned"
	RunStatusCostEstimating     RunStatus = "cost_estimating"
	RunStatusCostEstimated      RunStatus = "cost_estimated"
	RunStatusPolicyChecking     RunStatus = "policy_checking"
	RunStatusPolicyOverride     RunStatus = "policy_override"
	RunStatusPolicyChecked      RunStatus = "policy_checked"
	RunStatusConfirmed          RunStatus = "confirmed"
	RunStatusPlannedAndFinished RunStatus = "planned_and_finished"
	RunStatusApplyQueued        RunStatus = "apply_queued"
	RunStatusApplying           RunStatus = "applying"
	RunStatusApplied            RunStatus = "applied"
	RunStatusDiscarded          RunStatus = "discarded"
	RunStatusErrored            RunStatus = "errored"
	RunStatusCanceled           RunStatus = "canceled"
	RunStatusForceCanceled      RunStatus = "force_canceled"
)

// Client provides TFE API operations
type Client struct {
	address      string
	token        string
	organization string
}

// NewClient creates a new TFE client
func NewClient(address, token, organization string) *Client {
	return &Client{
		address:      address,
		token:        token,
		organization: organization,
	}
}

// WorkspaceCreateInput contains input for creating a workspace
type WorkspaceCreateInput struct {
	Name             string
	Organization     string
	TerraformVersion string
	WorkingDirectory string
	AutoApply        bool
	ExecutionMode    string
	AgentPoolID      string
	VCSRepo          string
	GitRepo          string
	Branch           string
}

// WorkspaceCreateOutput contains the result of workspace creation
type WorkspaceCreateOutput struct {
	ID            string
	Name          string
	TFEWorkspaceID string
	Created       bool
}

// RunCreateInput contains input for creating a run
type RunCreateInput struct {
	WorkspaceID string
	Message     string
	IsDestroy   bool
	AutoApply   bool
	PlanOnly    bool
	GitRepo     string
	Branch      string
}

// RunCreateOutput contains the result of run creation
type RunCreateOutput struct {
	RunID   string
	Status  string
	Message string
}

// RunStatusOutput contains the status of a run
type RunStatusOutput struct {
	RunID     string
	Status    RunStatus
	Message   string
	PlanURL   string
	ApplyURL  string
	HasChanges bool
}

// HookExecutionInput contains input for executing a hook
type HookExecutionInput struct {
	HookConfig   *pb.HookConfig
	DeploymentID string
	RunID        string
	RunStatus    string
	PlanURL      string
	ChangeSummary string
}

// HookExecutionOutput contains the result of hook execution
type HookExecutionOutput struct {
	Success bool
	Message string
}

// GetOrCreateWorkspace gets an existing workspace or creates a new one
func (c *Client) GetOrCreateWorkspace(ctx context.Context, input *WorkspaceCreateInput) (*WorkspaceCreateOutput, error) {
	// In a real implementation, this would call the TFE API
	// For now, we simulate the behavior
	
	org := input.Organization
	if org == "" {
		org = c.organization
	}

	// Simulate workspace ID generation
	workspaceID := fmt.Sprintf("ws-%s-%s-%d", org, input.Name, time.Now().UnixNano()%10000)
	
	return &WorkspaceCreateOutput{
		ID:             workspaceID,
		Name:           input.Name,
		TFEWorkspaceID: workspaceID,
		Created:        true, // In real implementation, this would be false if workspace already existed
	}, nil
}

// GetWorkspace retrieves a workspace by name
func (c *Client) GetWorkspace(ctx context.Context, organization, name string) (*WorkspaceCreateOutput, error) {
	// In a real implementation, this would call the TFE API
	// For now, we return nil to indicate workspace doesn't exist
	return nil, nil
}

// ConfigureWorkspaceVariables sets up workspace variables
func (c *Client) ConfigureWorkspaceVariables(ctx context.Context, workspaceID string, variables map[string]string) error {
	// In a real implementation, this would call the TFE API to set variables
	// For now, we just simulate success
	return nil
}

// CreateRun creates a new Terraform run
func (c *Client) CreateRun(ctx context.Context, input *RunCreateInput) (*RunCreateOutput, error) {
	// In a real implementation, this would:
	// 1. Create a configuration version
	// 2. Upload Terraform code
	// 3. Create and queue the run
	
	// Simulate run ID generation
	runID := fmt.Sprintf("run-%d", time.Now().UnixNano()%100000)
	
	return &RunCreateOutput{
		RunID:   runID,
		Status:  string(RunStatusPlanQueued),
		Message: "Run created and queued for planning",
	}, nil
}

// GetRunStatus retrieves the current status of a run
func (c *Client) GetRunStatus(ctx context.Context, runID string) (*RunStatusOutput, error) {
	// In a real implementation, this would call the TFE API
	// For now, we simulate a successful run
	
	return &RunStatusOutput{
		RunID:      runID,
		Status:     RunStatusApplied,
		Message:    "Apply complete",
		HasChanges: true,
	}, nil
}

// ConfirmRun confirms a run that is waiting for confirmation
func (c *Client) ConfirmRun(ctx context.Context, runID string) error {
	// In a real implementation, this would call the TFE API
	return nil
}

// CancelRun cancels a pending or running run
func (c *Client) CancelRun(ctx context.Context, runID string) error {
	// In a real implementation, this would call the TFE API
	return nil
}

// ExecuteHook executes a run lifecycle hook
func (c *Client) ExecuteHook(ctx context.Context, input *HookExecutionInput) (*HookExecutionOutput, error) {
	if input.HookConfig == nil || !input.HookConfig.Enabled {
		return &HookExecutionOutput{
			Success: true,
			Message: "Hook disabled, skipping",
		}, nil
	}

	// In a real implementation, this would:
	// 1. Call the webhook URL if configured
	// 2. Execute the script if configured
	// 3. Handle timeout and errors

	// Simulate successful hook execution
	return &HookExecutionOutput{
		Success: true,
		Message: "Hook executed successfully",
	}, nil
}

// IsTerminalStatus returns true if the run status is terminal (completed or failed)
func IsTerminalStatus(status RunStatus) bool {
	switch status {
	case RunStatusApplied,
		RunStatusPlannedAndFinished,
		RunStatusDiscarded,
		RunStatusErrored,
		RunStatusCanceled,
		RunStatusForceCanceled:
		return true
	default:
		return false
	}
}

// IsSuccessStatus returns true if the run status indicates success
func IsSuccessStatus(status RunStatus) bool {
	switch status {
	case RunStatusApplied, RunStatusPlannedAndFinished:
		return true
	default:
		return false
	}
}

// NeedsConfirmation returns true if the run is waiting for confirmation
func NeedsConfirmation(status RunStatus) bool {
	switch status {
	case RunStatusPlanned, RunStatusCostEstimated, RunStatusPolicyChecked:
		return true
	default:
		return false
	}
}
