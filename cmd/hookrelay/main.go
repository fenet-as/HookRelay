package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fenet-as/hookrelay/internal/api"
	"github.com/fenet-as/hookrelay/internal/config"
	"github.com/fenet-as/hookrelay/internal/delivery"
	"github.com/fenet-as/hookrelay/internal/store"
)

func main() {
	cfg := config.Load()

	// Initialize structured logger
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	logger.Info("starting hookrelay service",
		slog.String("port", cfg.Port),
		slog.String("env", cfg.Env),
	)

	// 1. Root context tied to OS termination signals (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Connect to PostgreSQL
	dbPool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: cfg.DatabaseURL,
		MaxConns:    20,
	})
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	logger.Info("connected to PostgreSQL successfully")

	queries := store.New(dbPool)

	// 3. Initialize Outbound Delivery Client, Dispatcher, Worker Pool, and Crash Recovery Reaper
	clientCfg := delivery.DefaultClientConfig()
	clientCfg.AllowLocal = cfg.AllowLocal
	client := delivery.NewClient(clientCfg)
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	workerPoolCfg := delivery.DefaultPoolConfig()
	deliveryPool := delivery.NewPool(workerPoolCfg, dispatcher, queries, logger)
	deliveryPool.Start(ctx)

	reaperCfg := delivery.DefaultReaperConfig()
	reaper := delivery.NewReaper(reaperCfg, queries, logger)
	go reaper.Start(ctx)

	// 4. Initialize API server
	server := api.NewServer(dbPool, logger)

	addr := fmt.Sprintf(":%s", cfg.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: server.Router(),
	}

	go func() {
		logger.Info("HTTP server listening", slog.String("addr", addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", "error", err)
			stop()
		}
	}()

	// 5. Wait for OS Termination Signal
	<-ctx.Done()
	logger.Info("shutdown signal received, initiating graceful shutdown...")

	// Step A: Stop accepting new HTTP requests (10s deadline)
	shutdownCtx, cancelHTTP := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHTTP()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server forced to shutdown", "error", err)
	} else {
		logger.Info("HTTP server stopped cleanly")
	}

	// Step B: Stop worker pool and wait for in-flight tasks to finish
	logger.Info("stopping delivery worker pool...")
	deliveryPool.Stop()

	// Step C: Close idle keep-alive connections in HTTP transport
	client.CloseIdleConnections()

	logger.Info("service shutdown complete")
}
