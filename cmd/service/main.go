package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OlegGorj/tfe-deployment-service/internal/config"
	"github.com/OlegGorj/tfe-deployment-service/pkg/activities"
	"github.com/OlegGorj/tfe-deployment-service/pkg/db"
	"github.com/OlegGorj/tfe-deployment-service/pkg/handlers"
	"github.com/OlegGorj/tfe-deployment-service/pkg/tfe"
	"github.com/OlegGorj/tfe-deployment-service/pkg/workflows"
	"github.com/gin-gonic/gin"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Initialize in-memory database
	database := db.NewInMemoryDB()
	database.SeedAppRegistry() // Seed with sample data

	// Initialize TFE client
	tfeClient := tfe.NewClient(cfg.TFE.Address, cfg.TFE.Token, cfg.TFE.Organization)

	// Initialize Temporal client
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Host,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.Printf("Warning: Failed to create Temporal client: %v", err)
		log.Printf("Service will start but workflow functionality will be disabled")
		temporalClient = nil
	}

	// Create activities
	act := activities.NewActivities(database, tfeClient)

	// Start Temporal worker if client is available
	var w worker.Worker
	if temporalClient != nil {
		w = worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{})
		w.RegisterWorkflow(workflows.DeploymentWorkflow)
		w.RegisterActivity(act.WorkspaceActivity)
		w.RegisterActivity(act.TFRunActivity)
		w.RegisterActivity(act.PollRunStatusActivity)
		w.RegisterActivity(act.UpdateDeploymentStatusActivity)
		w.RegisterActivity(act.ExecuteHookActivity)
		w.RegisterActivity(act.LookupRegistryActivity)
		w.RegisterActivity(act.CreateDeploymentActivity)

		go func() {
			if err := w.Run(worker.InterruptCh()); err != nil {
				log.Printf("Temporal worker failed: %v", err)
			}
		}()
	}

	// Create handlers
	h := handlers.NewHandlers(database, temporalClient, cfg.Temporal.TaskQueue)

	// Setup Gin router
	router := gin.Default()

	// API endpoints
	router.POST("/deploy", h.Deploy)
	router.GET("/status/:id", h.GetStatus)
	router.POST("/callback", h.Callback)
	router.GET("/poll/:id", h.Poll)

	// Health check endpoints
	router.GET("/health", h.Health)
	router.GET("/ready", h.Ready)

	// Create HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Starting TFE Deployment Service on port %d", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	if w != nil {
		w.Stop()
	}

	if temporalClient != nil {
		temporalClient.Close()
	}

	log.Println("Server exited")
}
