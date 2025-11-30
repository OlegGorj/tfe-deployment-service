package workflows

import (
	"fmt"
	"time"

	pb "github.com/OlegGorj/tfe-deployment-service/api/gen/deployment/v1"
	"github.com/OlegGorj/tfe-deployment-service/pkg/activities"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// DeploymentWorkflowInput contains input for the deployment workflow
type DeploymentWorkflowInput struct {
	DeploymentID string
	GitRepo      string
	Branch       string
	AppName      string
	Environment  string
	RunType      pb.RunType
	Registry     *pb.AppRegistry
}

// DeploymentWorkflowOutput contains output from the deployment workflow
type DeploymentWorkflowOutput struct {
	Success    bool
	Status     pb.DeploymentStatus
	Message    string
	TFERunID   string
	WorkspaceID string
}

// DeploymentWorkflow orchestrates the entire deployment process
func DeploymentWorkflow(ctx workflow.Context, input DeploymentWorkflowInput) (*DeploymentWorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting deployment workflow", "deploymentID", input.DeploymentID, "appName", input.AppName, "environment", input.Environment)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var act *activities.Activities

	// Step 1: Update deployment status to running
	err := workflow.ExecuteActivity(ctx, act.UpdateDeploymentStatusActivity, activities.UpdateDeploymentStatusInput{
		DeploymentID: input.DeploymentID,
		Status:       pb.DeploymentStatus_DEPLOYMENT_STATUS_RUNNING,
		Message:      "Deployment started",
	}).Get(ctx, nil)
	if err != nil {
		return handleWorkflowError(ctx, act, input.DeploymentID, "Failed to update deployment status", err)
	}

	// Step 2: Create or get workspace
	var workspaceOutput activities.WorkspaceActivityOutput
	err = workflow.ExecuteActivity(ctx, act.WorkspaceActivity, activities.WorkspaceActivityInput{
		DeploymentID:     input.DeploymentID,
		AppName:          input.AppName,
		Environment:      input.Environment,
		Organization:     input.Registry.WorkspaceConfig.Organization,
		WorkspacePrefix:  input.Registry.WorkspaceConfig.WorkspacePrefix,
		TerraformVersion: input.Registry.WorkspaceConfig.TerraformVersion,
		WorkingDirectory: input.Registry.WorkspaceConfig.WorkingDirectory,
		ExecutionMode:    input.Registry.WorkspaceConfig.ExecutionMode,
		AgentPoolID:      input.Registry.WorkspaceConfig.AgentPoolId,
		VCSRepo:          input.Registry.WorkspaceConfig.VcsRepo,
		GitRepo:          input.GitRepo,
		Branch:           input.Branch,
		Variables:        input.Registry.Variables,
	}).Get(ctx, &workspaceOutput)
	if err != nil {
		return handleWorkflowError(ctx, act, input.DeploymentID, "Failed to create/get workspace", err)
	}

	logger.Info("Workspace ready", "workspaceID", workspaceOutput.WorkspaceID, "created", workspaceOutput.Created)

	// Step 3: Create TFE run
	var runOutput activities.TFRunActivityOutput
	err = workflow.ExecuteActivity(ctx, act.TFRunActivity, activities.TFRunActivityInput{
		DeploymentID:   input.DeploymentID,
		WorkspaceID:    workspaceOutput.WorkspaceID,
		TFEWorkspaceID: workspaceOutput.TFEWorkspaceID,
		RunType:        input.RunType,
		GitRepo:        input.GitRepo,
		Branch:         input.Branch,
		Message:        fmt.Sprintf("Deployment %s for %s/%s", input.DeploymentID, input.AppName, input.Environment),
		AutoApply:      input.Registry.WorkspaceConfig.AutoApply,
	}).Get(ctx, &runOutput)
	if err != nil {
		return handleWorkflowError(ctx, act, input.DeploymentID, "Failed to create TFE run", err)
	}

	logger.Info("TFE run created", "runID", runOutput.TFERunID)

	// Step 4: Poll for run completion with hooks
	pollOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1.5,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	}
	pollCtx := workflow.WithActivityOptions(ctx, pollOptions)

	var finalStatus activities.PollRunStatusOutput
	postPlanExecuted := false
	preApplyExecuted := false

	// Poll loop with 1-hour timeout
	pollTimeout := workflow.NewTimer(ctx, time.Hour)
	pollInterval := 30 * time.Second

	for {
		// Check for timeout
		timerFired := false
		workflow.NewSelector(ctx).
			AddFuture(pollTimeout, func(f workflow.Future) {
				timerFired = true
			}).
			AddFuture(workflow.NewTimer(ctx, pollInterval), func(f workflow.Future) {}).
			Select(ctx)

		if timerFired {
			return handleWorkflowError(ctx, act, input.DeploymentID, "Deployment timed out after 1 hour", nil)
		}

		// Poll run status
		var statusOutput activities.PollRunStatusOutput
		err = workflow.ExecuteActivity(pollCtx, act.PollRunStatusActivity, activities.PollRunStatusInput{
			DeploymentID: input.DeploymentID,
			TFERunID:     runOutput.TFERunID,
		}).Get(ctx, &statusOutput)
		if err != nil {
			logger.Warn("Failed to poll run status, will retry", "error", err)
			continue
		}

		logger.Info("Run status", "status", statusOutput.Status, "isTerminal", statusOutput.IsTerminal)

		// Execute post-plan hook after planning completes
		if !postPlanExecuted && shouldExecutePostPlanHook(statusOutput.Status) {
			postPlanExecuted = true
			if input.Registry.RunHooks != nil && input.Registry.RunHooks.PostPlan != nil {
				var hookOutput activities.ExecuteHookOutput
				err = workflow.ExecuteActivity(ctx, act.ExecuteHookActivity, activities.ExecuteHookInput{
					DeploymentID:  input.DeploymentID,
					HookType:      "post_plan",
					HookConfig:    input.Registry.RunHooks.PostPlan,
					RunID:         runOutput.TFERunID,
					RunStatus:     statusOutput.Status,
					PlanURL:       statusOutput.PlanURL,
					ChangeSummary: "{}",
				}).Get(ctx, &hookOutput)
				if err != nil && input.Registry.RunHooks.PostPlan.FailOnError {
					return handleWorkflowError(ctx, act, input.DeploymentID, "Post-plan hook failed", err)
				}
				logger.Info("Post-plan hook executed", "success", hookOutput.Success)
			}
		}

		// Execute pre-apply hook before apply starts
		if !preApplyExecuted && shouldExecutePreApplyHook(statusOutput.Status) {
			preApplyExecuted = true
			if input.Registry.RunHooks != nil && input.Registry.RunHooks.PreApply != nil {
				var hookOutput activities.ExecuteHookOutput
				err = workflow.ExecuteActivity(ctx, act.ExecuteHookActivity, activities.ExecuteHookInput{
					DeploymentID: input.DeploymentID,
					HookType:     "pre_apply",
					HookConfig:   input.Registry.RunHooks.PreApply,
					RunID:        runOutput.TFERunID,
					RunStatus:    statusOutput.Status,
				}).Get(ctx, &hookOutput)
				if err != nil && input.Registry.RunHooks.PreApply.FailOnError {
					return handleWorkflowError(ctx, act, input.DeploymentID, "Pre-apply hook failed", err)
				}
				logger.Info("Pre-apply hook executed", "success", hookOutput.Success)
			}
		}

		// Check if run is terminal
		if statusOutput.IsTerminal {
			finalStatus = statusOutput
			break
		}
	}

	// Step 5: Execute post-apply hook
	if input.Registry.RunHooks != nil && input.Registry.RunHooks.PostApply != nil {
		var hookOutput activities.ExecuteHookOutput
		err = workflow.ExecuteActivity(ctx, act.ExecuteHookActivity, activities.ExecuteHookInput{
			DeploymentID: input.DeploymentID,
			HookType:     "post_apply",
			HookConfig:   input.Registry.RunHooks.PostApply,
			RunID:        runOutput.TFERunID,
			RunStatus:    finalStatus.Status,
		}).Get(ctx, &hookOutput)
		if err != nil && input.Registry.RunHooks.PostApply.FailOnError {
			return handleWorkflowError(ctx, act, input.DeploymentID, "Post-apply hook failed", err)
		}
		logger.Info("Post-apply hook executed", "success", hookOutput.Success)
	}

	// Step 6: Update final deployment status
	var finalDeploymentStatus pb.DeploymentStatus
	var finalMessage string

	if finalStatus.IsSuccess {
		finalDeploymentStatus = pb.DeploymentStatus_DEPLOYMENT_STATUS_SUCCESS
		finalMessage = fmt.Sprintf("Deployment completed successfully: %s", finalStatus.Message)
	} else {
		finalDeploymentStatus = pb.DeploymentStatus_DEPLOYMENT_STATUS_FAILED
		finalMessage = fmt.Sprintf("Deployment failed: %s", finalStatus.Message)
	}

	err = workflow.ExecuteActivity(ctx, act.UpdateDeploymentStatusActivity, activities.UpdateDeploymentStatusInput{
		DeploymentID: input.DeploymentID,
		Status:       finalDeploymentStatus,
		Message:      finalMessage,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to update final deployment status", "error", err)
	}

	logger.Info("Deployment workflow completed", "status", finalDeploymentStatus, "message", finalMessage)

	return &DeploymentWorkflowOutput{
		Success:     finalStatus.IsSuccess,
		Status:      finalDeploymentStatus,
		Message:     finalMessage,
		TFERunID:    runOutput.TFERunID,
		WorkspaceID: workspaceOutput.WorkspaceID,
	}, nil
}

// handleWorkflowError handles errors in the workflow and updates deployment status
func handleWorkflowError(ctx workflow.Context, act *activities.Activities, deploymentID string, message string, err error) (*DeploymentWorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	
	errorMessage := message
	if err != nil {
		errorMessage = fmt.Sprintf("%s: %v", message, err)
	}
	
	logger.Error("Deployment workflow error", "deploymentID", deploymentID, "error", errorMessage)

	// Try to update deployment status to failed
	updateErr := workflow.ExecuteActivity(ctx, act.UpdateDeploymentStatusActivity, activities.UpdateDeploymentStatusInput{
		DeploymentID: deploymentID,
		Status:       pb.DeploymentStatus_DEPLOYMENT_STATUS_FAILED,
		Message:      errorMessage,
	}).Get(ctx, nil)
	if updateErr != nil {
		logger.Warn("Failed to update deployment status on error", "error", updateErr)
	}

	return &DeploymentWorkflowOutput{
		Success: false,
		Status:  pb.DeploymentStatus_DEPLOYMENT_STATUS_FAILED,
		Message: errorMessage,
	}, fmt.Errorf("%s", errorMessage)
}

// shouldExecutePostPlanHook returns true if the run status indicates plan completion
func shouldExecutePostPlanHook(status string) bool {
	return status == "planned" || status == "planned_and_finished" || 
		   status == "cost_estimated" || status == "policy_checked"
}

// shouldExecutePreApplyHook returns true if the run status indicates apply is about to start
func shouldExecutePreApplyHook(status string) bool {
	return status == "confirmed" || status == "apply_queued"
}
