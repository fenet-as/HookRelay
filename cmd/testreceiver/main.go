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
)

func main() {
	port := getEnv("PORT", "9090")
	defaultMode := getEnv("RECEIVER_MODE", "ok")
	secret := getEnv("RECEIVER_SECRET", "")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := ReceiverConfig{
		Port:         port,
		DefaultMode:  defaultMode,
		Secret:       secret,
		SlowDuration: 3 * time.Second,
	}

	handler := NewReceiverHandler(cfg, logger)

	addr := fmt.Sprintf(":%s", port)
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("test receiver listening",
			slog.String("addr", addr),
			slog.String("default_mode", defaultMode),
			slog.Bool("verify_signatures", secret != ""),
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("test receiver failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down test receiver...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("test receiver forced to shutdown", "error", err)
	} else {
		logger.Info("test receiver stopped cleanly")
	}
}
