package api

import (
	"log/slog"

	"github.com/fenet-as/hookrelay/internal/fanout"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/fenet-as/hookrelay/internal/telemetry"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server coordinates HTTP routing, handlers, fan-out engine, and persistence.
type Server struct {
	router  *gin.Engine
	queries *store.Queries
	pool    *pgxpool.Pool
	logger  *slog.Logger
	fanout  *fanout.Engine
	metrics *telemetry.Metrics
}

// NewServer builds and configures a new HTTP Server instance.
func NewServer(pool *pgxpool.Pool, logger *slog.Logger, metrics ...*telemetry.Metrics) *Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	var m *telemetry.Metrics
	if len(metrics) > 0 {
		m = metrics[0]
	}

	s := &Server{
		router:  router,
		queries: store.New(pool),
		pool:    pool,
		logger:  logger,
		fanout:  fanout.NewEngine(pool),
		metrics: m,
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

	if s.metrics != nil {
		s.router.GET("/metrics", gin.WrapH(s.metrics.HTTPHandler()))
	}

	endpointsGroup := s.router.Group("/endpoints")
	{
		endpointsGroup.POST("", s.CreateEndpointHandler)
		endpointsGroup.GET("", s.ListEndpointsHandler)
		endpointsGroup.GET("/:id", s.GetEndpointByIDHandler)
		endpointsGroup.PATCH("/:id", s.UpdateEndpointHandler)
		endpointsGroup.DELETE("/:id", s.DeleteEndpointHandler)
		endpointsGroup.POST("/:id/rotate-secret", s.RotateSecretHandler)
	}

	eventsGroup := s.router.Group("/events")
	{
		eventsGroup.POST("", s.PublishEventHandler)
		eventsGroup.GET("/:id", s.GetEventByIDHandler)
	}

	deliveriesGroup := s.router.Group("/deliveries")
	{
		deliveriesGroup.GET("/:id", s.GetDeliveryByIDHandler)
		deliveriesGroup.POST("/:id/replay", s.ReplayDeliveryHandler)
	}
}
