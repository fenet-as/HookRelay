package api

import (
	"log/slog"

	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server coordinates HTTP routing, handlers, and persistence.
type Server struct {
	router  *gin.Engine
	queries *store.Queries
	pool    *pgxpool.Pool
	logger  *slog.Logger
}

// NewServer builds and configures a new HTTP Server instance.
func NewServer(pool *pgxpool.Pool, logger *slog.Logger) *Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	s := &Server{
		router:  router,
		queries: store.New(pool),
		pool:    pool,
		logger:  logger,
	}

	// Attach custom slog logging and panic recovery
	router.Use(SlogMiddleware(logger))
	router.Use(SlogRecovery(logger))

	// Register routes
	s.registerRoutes()

	return s
}

// Router returns the underlying gin.Engine for running or testing.
func (s *Server) Router() *gin.Engine {
	return s.router
}

func (s *Server) registerRoutes() {
	s.router.GET("/healthz", s.HealthzHandler)

	endpointsGroup := s.router.Group("/endpoints")
	{
		endpointsGroup.POST("", s.CreateEndpointHandler)
		endpointsGroup.GET("", s.ListEndpointsHandler)
		endpointsGroup.GET("/:id", s.GetEndpointByIDHandler)
		endpointsGroup.PATCH("/:id", s.UpdateEndpointHandler)
		endpointsGroup.DELETE("/:id", s.DeleteEndpointHandler)
		endpointsGroup.POST("/:id/rotate-secret", s.RotateSecretHandler)
	}
}
