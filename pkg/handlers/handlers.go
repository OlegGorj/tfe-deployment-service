package handlers

import (
	"context"
	"fmt"
	"net/http"

	pb "github.com/OlegGorj/tfe-deployment-service/api/gen/deployment/v1"
	"github.com/OlegGorj/tfe-deployment-service/pkg/db"
	"github.com/OlegGorj/tfe-deployment-service/pkg/workflows"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
)

// Handlers contains HTTP handlers and their dependencies
type Handlers struct {
	DB             *db.InMemoryDB
	TemporalClient client.Client
	TaskQueue      string
}

// NewHandlers creates a new Handlers instance
func NewHandlers(database *db.InMemoryDB, temporalClient client.Client, taskQueue string) *Handlers {
	return &Handlers{
		DB:             database,
		TemporalClient: temporalClient,
		TaskQueue:      taskQueue,
	}
}

// DeployRequest represents the JSON request body for deploy endpoint
type DeployRequest struct {
	GitRepo     string `json:"git_repo" binding:"required"`
	Branch      string `json:"branch" binding:"required"`
	AppName     string `json:"app_name" binding:"required"`
	Environment string `json:"environment" binding:"required"`
	RunType     string `json:"run_type" binding:"required"`
}

// DeployResponse represents the JSON response for deploy endpoint
type DeployResponse struct {
	DeploymentID string `json:"deployment_id"`
	Status       string `json:"status"`
	Message      string `json:"message"`
}

// StatusResponse represents the JSON response for status endpoint
type StatusResponse struct {
	DeploymentID string `json:"deployment_id"`
	Status       string `json:"status"`
	GitRepo      string `json:"git_repo"`
	Branch       string `json:"branch"`
	AppName      string `json:"app_name"`
	Environment  string `json:"environment"`
	RunType      string `json:"run_type"`
	WorkspaceID  string `json:"workspace_id,omitempty"`
	TFERunID     string `json:"tfe_run_id,omitempty"`
	Message      string `json:"message,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// CallbackRequest represents the JSON request body for callback endpoint
type CallbackRequest struct {
	RunID      string `json:"run_id" binding:"required"`
	RunStatus  string `json:"run_status" binding:"required"`
	Message    string `json:"message"`
	WorkflowID string `json:"workflow_id"`
}

// CallbackResponse represents the JSON response for callback endpoint
type CallbackResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// PollResponse represents the JSON response for poll endpoint
type PollResponse struct {
	DeploymentID string `json:"deployment_id"`
	Status       string `json:"status"`
	TFERunID     string `json:"tfe_run_id,omitempty"`
	TFERunStatus string `json:"tfe_run_status,omitempty"`
	Message      string `json:"message,omitempty"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Deploy handles POST /deploy requests
func (h *Handlers) Deploy(c *gin.Context) {
	var req DeployRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "validation_error",
			Message: err.Error(),
		})
		return
	}

	// Validate run type
	runType, err := parseRunType(req.RunType)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_run_type",
			Message: err.Error(),
		})
		return
	}

	// Lookup application in registry
	registry, err := h.DB.GetAppRegistry(req.AppName, req.Environment)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error:   "registry_not_found",
			Message: "Application not registered for this environment. Please onboard the application first.",
		})
		return
	}

	// Generate deployment ID
	deploymentID := uuid.New().String()

	// Create deployment record
	deployment := &pb.Deployment{
		Id:          deploymentID,
		GitRepo:     req.GitRepo,
		Branch:      req.Branch,
		AppName:     req.AppName,
		Environment: req.Environment,
		RunType:     runType,
		Status:      pb.DeploymentStatus_DEPLOYMENT_STATUS_PENDING,
	}

	if err := h.DB.CreateDeployment(deployment); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "database_error",
			Message: "Failed to create deployment record",
		})
		return
	}

	// Check if Temporal client is available
	if h.TemporalClient == nil {
		// In mock mode, just return the deployment ID
		c.JSON(http.StatusAccepted, DeployResponse{
			DeploymentID: deploymentID,
			Status:       "pending",
			Message:      "Deployment created (workflow disabled - Temporal not available)",
		})
		return
	}

	// Start Temporal workflow
	workflowOptions := client.StartWorkflowOptions{
		ID:        "deployment-" + deploymentID,
		TaskQueue: h.TaskQueue,
	}

	workflowInput := workflows.DeploymentWorkflowInput{
		DeploymentID: deploymentID,
		GitRepo:      req.GitRepo,
		Branch:       req.Branch,
		AppName:      req.AppName,
		Environment:  req.Environment,
		RunType:      runType,
		Registry:     registry,
	}

	_, err = h.TemporalClient.ExecuteWorkflow(context.Background(), workflowOptions, workflows.DeploymentWorkflow, workflowInput)
	if err != nil {
		// Update deployment status to failed
		_ = h.DB.UpdateDeploymentStatus(deploymentID, pb.DeploymentStatus_DEPLOYMENT_STATUS_FAILED, "Failed to start workflow")
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "workflow_error",
			Message: "Failed to start deployment workflow",
		})
		return
	}

	c.JSON(http.StatusAccepted, DeployResponse{
		DeploymentID: deploymentID,
		Status:       "pending",
		Message:      "Deployment initiated successfully",
	})
}

// GetStatus handles GET /status/:id requests
func (h *Handlers) GetStatus(c *gin.Context) {
	deploymentID := c.Param("id")
	if deploymentID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "missing_id",
			Message: "Deployment ID is required",
		})
		return
	}

	deployment, err := h.DB.GetDeployment(deploymentID)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error:   "not_found",
			Message: "Deployment not found",
		})
		return
	}

	c.JSON(http.StatusOK, StatusResponse{
		DeploymentID: deployment.Id,
		Status:       deploymentStatusToString(deployment.Status),
		GitRepo:      deployment.GitRepo,
		Branch:       deployment.Branch,
		AppName:      deployment.AppName,
		Environment:  deployment.Environment,
		RunType:      runTypeToString(deployment.RunType),
		WorkspaceID:  deployment.WorkspaceId,
		TFERunID:     deployment.TfeRunId,
		Message:      deployment.Message,
		CreatedAt:    deployment.CreatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:    deployment.UpdatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"),
	})
}

// Callback handles POST /callback requests from TFE
func (h *Handlers) Callback(c *gin.Context) {
	var req CallbackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "validation_error",
			Message: err.Error(),
		})
		return
	}

	// Find deployment by TFE run ID
	deployments := h.DB.ListDeployments()
	var foundDeployment *pb.Deployment
	for _, d := range deployments {
		if d.TfeRunId == req.RunID {
			foundDeployment = d
			break
		}
	}

	if foundDeployment == nil {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error:   "not_found",
			Message: "No deployment found for this run ID",
		})
		return
	}

	// Signal the workflow if workflow ID is provided and Temporal client is available
	if req.WorkflowID != "" && h.TemporalClient != nil {
		err := h.TemporalClient.SignalWorkflow(context.Background(), req.WorkflowID, "", "tfe-callback", req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Error:   "signal_error",
				Message: "Failed to signal workflow",
			})
			return
		}
	}

	c.JSON(http.StatusOK, CallbackResponse{
		Success: true,
		Message: "Callback processed successfully",
	})
}

// Poll handles GET /poll/:id requests
func (h *Handlers) Poll(c *gin.Context) {
	deploymentID := c.Param("id")
	if deploymentID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "missing_id",
			Message: "Deployment ID is required",
		})
		return
	}

	deployment, err := h.DB.GetDeployment(deploymentID)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error:   "not_found",
			Message: "Deployment not found",
		})
		return
	}

	c.JSON(http.StatusOK, PollResponse{
		DeploymentID: deployment.Id,
		Status:       deploymentStatusToString(deployment.Status),
		TFERunID:     deployment.TfeRunId,
		TFERunStatus: "", // Would be populated from TFE in real implementation
		Message:      deployment.Message,
	})
}

// Health handles GET /health requests
func (h *Handlers) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

// Ready handles GET /ready requests
func (h *Handlers) Ready(c *gin.Context) {
	// Check Temporal connection
	// In a real implementation, this would verify the connection
	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
	})
}

// Helper functions

func parseRunType(s string) (pb.RunType, error) {
	switch s {
	case "plan":
		return pb.RunType_RUN_TYPE_PLAN, nil
	case "plan-apply":
		return pb.RunType_RUN_TYPE_PLAN_APPLY, nil
	case "plan-destroy":
		return pb.RunType_RUN_TYPE_PLAN_DESTROY, nil
	case "destroy":
		return pb.RunType_RUN_TYPE_DESTROY, nil
	default:
		return pb.RunType_RUN_TYPE_UNSPECIFIED, fmt.Errorf("invalid run type: %s, must be one of: plan, plan-apply, plan-destroy, destroy", s)
	}
}

func deploymentStatusToString(status pb.DeploymentStatus) string {
	switch status {
	case pb.DeploymentStatus_DEPLOYMENT_STATUS_PENDING:
		return "pending"
	case pb.DeploymentStatus_DEPLOYMENT_STATUS_RUNNING:
		return "running"
	case pb.DeploymentStatus_DEPLOYMENT_STATUS_SUCCESS:
		return "success"
	case pb.DeploymentStatus_DEPLOYMENT_STATUS_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

func runTypeToString(rt pb.RunType) string {
	switch rt {
	case pb.RunType_RUN_TYPE_PLAN:
		return "plan"
	case pb.RunType_RUN_TYPE_PLAN_APPLY:
		return "plan-apply"
	case pb.RunType_RUN_TYPE_PLAN_DESTROY:
		return "plan-destroy"
	case pb.RunType_RUN_TYPE_DESTROY:
		return "destroy"
	default:
		return "unknown"
	}
}
