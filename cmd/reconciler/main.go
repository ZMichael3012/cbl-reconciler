// main starts the reconciler service: the administrative HTTP and metrics
// servers, the Kafka consumers, the outbox relay and the periodic recon pass.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-reconciler/internal/storage"
	"github.com/cbl/cbl-reconciler/internal/transport"
	"github.com/cbl/cbl-reconciler/internal/usecase"
)

// main brings up the service: storage with migrations, the Kafka contours and
// the periodic reconciliation pass.
func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	httpAddr := getenv("HTTP_ADDR", ":8100")
	grpcAddr := getenv("GRPC_ADDR", ":9100")
	metricsAddr := getenv("METRICS_ADDR", ":9300")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := gorm.Open(postgres.Open(os.Getenv("PG_DSN")), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Error("postgres open failed", zap.Error(err))
		return
	}
	if err := storagex.Migrate(ctx, db, getenv("MIGRATIONS_DIR", "./migrations")); err != nil {
		log.Error("apply migrations failed", zap.Error(err))
		return
	}

	// Kafka contours: the outbox relay, the canonical observation consumer and
	// the payout verdict consumer.
	brokers := splitCSV(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) > 0 {
		publisher, pubErr := kafkapub.New(brokers)
		if pubErr != nil {
			log.Error("kafka publisher failed", zap.Error(pubErr))
			return
		}
		defer publisher.Close()
		relay := outbox.NewRelay(db, publisher, log)
		go func() {
			if relayErr := relay.Run(ctx); relayErr != nil && !errors.Is(relayErr, context.Canceled) {
				log.Error("outbox relay stopped", zap.Error(relayErr))
			}
		}()

		canonicalGroup := getenv("RECON_CANONICAL_CONSUMER_GROUP", usecase.ConsumerGroupCanonical)
		canonicalProcessor := inbox.NewProcessor(canonicalGroup, db, inbox.NewGuard(),
			usecase.NewCanonicalHandler(storage.NewObservationRepository(db), log),
			publisher, inbox.NewRouter(usecase.TopicGatewayEventsCanonical, 3), log)
		canonicalRunner, runErr := kafkaconsumer.New(brokers, canonicalGroup,
			topicWithRetries(usecase.TopicGatewayEventsCanonical), canonicalProcessor, log)
		if runErr != nil {
			log.Error("canonical consumer failed", zap.Error(runErr))
			return
		}
		defer canonicalRunner.Close()
		go func() {
			if runErr := canonicalRunner.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("canonical consumer stopped", zap.Error(runErr))
			}
		}()

		payoutGroup := getenv("RECON_PAYOUT_CONSUMER_GROUP", usecase.ConsumerGroupPayouts)
		payoutProcessor := inbox.NewProcessor(payoutGroup, db, inbox.NewGuard(),
			usecase.NewPayoutHandler(storage.NewObservationRepository(db), log),
			publisher, inbox.NewRouter(usecase.TopicGatewayPayoutEvents, 3), log)
		payoutRunner, runErr := kafkaconsumer.New(brokers, payoutGroup,
			topicWithRetries(usecase.TopicGatewayPayoutEvents), payoutProcessor, log)
		if runErr != nil {
			log.Error("payout consumer failed", zap.Error(runErr))
			return
		}
		defer payoutRunner.Close()
		go func() {
			if runErr := payoutRunner.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("payout consumer stopped", zap.Error(runErr))
			}
		}()
	} else {
		log.Warn("kafka is not configured: consumers and the outbox relay are disabled")
	}

	// The periodic recon pass runs when the gRPC targets are configured.
	stopPass := startPass(ctx, db, log)
	defer stopPass()

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		sqlDB, dbErr := db.DB()
		if dbErr != nil || sqlDB.PingContext(c.Request.Context()) != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "storage unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	httpSrv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}

	grpcSrv := grpc.NewServer()
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	reflection.Register(grpcSrv)

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("grpc listen failed", zap.String("addr", grpcAddr), zap.Error(err))
		return
	}

	go func() {
		log.Info("http server started", zap.String("addr", httpAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", metricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("grpc server started", zap.String("addr", grpcAddr))
		if serveErr := grpcSrv.Serve(grpcListener); serveErr != nil {
			log.Error("grpc server failed", zap.Error(serveErr))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	grpcSrv.GracefulStop()
	log.Info("shutdown finished")
}

// startPass builds the gRPC clients and the pass worker; it returns a stop
// function closing the clients. Without the configured targets the pass stays
// disabled (fail closed) and the service still consumes the topics.
func startPass(ctx context.Context, db *gorm.DB, log *zap.Logger) func() {
	connectorAddr := strings.TrimSpace(os.Getenv("RECON_CONNECTOR_ADDR"))
	paymentAddr := strings.TrimSpace(os.Getenv("RECON_PAYMENT_ADDR"))
	ledgerAddr := strings.TrimSpace(os.Getenv("RECON_LEDGER_ADDR"))
	providers := splitCSV(os.Getenv("RECON_PROVIDERS"))
	interval := durationValue(os.Getenv("RECON_EXPORT_INTERVAL"), 5*time.Minute)
	if connectorAddr == "" || paymentAddr == "" || ledgerAddr == "" || len(providers) == 0 {
		log.Warn("recon pass is disabled: connector, payment, ledger addresses and providers are required")
		return func() {}
	}
	connectorClient, err := transport.DialConnector(connectorAddr)
	if err != nil {
		log.Error("connector client failed", zap.Error(err))
		return func() {}
	}
	paymentClient, err := transport.DialPayment(paymentAddr)
	if err != nil {
		log.Error("payment client failed", zap.Error(err))
		_ = connectorClient.Close()
		return func() {}
	}
	ledgerPageLimit, _ := strconv.Atoi(getenv("RECON_LEDGER_PAGE_LIMIT", "200"))
	ledgerMaxPages, _ := strconv.Atoi(getenv("RECON_LEDGER_MAX_PAGES", "10"))
	ledgerClient, err := transport.DialLedger(ledgerAddr, uint32(ledgerPageLimit), ledgerMaxPages)
	if err != nil {
		log.Error("ledger client failed", zap.Error(err))
		_ = connectorClient.Close()
		_ = paymentClient.Close()
		return func() {}
	}
	observations := storage.NewObservationRepository(db)
	pass, err := usecase.NewReconPass(
		connectorClient,
		paymentClient,
		ledgerClient,
		storage.NewAlertStore(db),
		storage.NewReplayStore(db),
		observations,
		storage.NewRunRepository(db),
		usecase.Config{
			Providers:       providers,
			PageLimit:       intValue(getenv("RECON_PAGE_LIMIT", "100"), 100),
			Lookback:        durationValue(os.Getenv("RECON_LOOKBACK"), 24*time.Hour),
			ExportMaxPages:  intValue(getenv("RECON_EXPORT_MAX_PAGES", "100"), 100),
			RepairGrace:     durationValue(os.Getenv("RECON_REPAIR_GRACE"), 15*time.Minute),
			LedgerPageLimit: ledgerPageLimit,
			LedgerMaxPages:  ledgerMaxPages,
			Log:             log,
		},
	)
	if err != nil {
		log.Error("recon pass configuration failed", zap.Error(err))
		_ = connectorClient.Close()
		_ = paymentClient.Close()
		_ = ledgerClient.Close()
		return func() {}
	}
	if interval > 0 {
		go func() {
			run := func() {
				report, runErr := pass.RunOnce(ctx)
				if runErr != nil {
					log.Error("recon pass failed", zap.Error(runErr))
				}
				for _, provider := range report.Providers {
					log.Info("recon pass finished",
						zap.String("provider", provider.Provider),
						zap.Int("pages", provider.Pages),
						zap.Int("rows", provider.Rows),
						zap.Int("replayed", provider.Replayed),
						zap.Int("alerts", provider.Alerts),
					)
				}
			}
			run()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					run()
				}
			}
		}()
	}
	return func() {
		_ = connectorClient.Close()
		_ = paymentClient.Close()
		_ = ledgerClient.Close()
	}
}

// topicWithRetries expands a main topic with the kit retry suffixes.
func topicWithRetries(main string) []string {
	return []string{
		main,
		main + inbox.RetrySuffix5s,
		main + inbox.RetrySuffix1m,
		main + inbox.RetrySuffix10m,
	}
}

// metricsHandler serves the metrics placeholder until task 0.7.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# metrics endpoint placeholder (task 0.7)")
	})
	return mux
}

// getenv returns the environment variable value or the default value.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// splitCSV splits a comma separated environment value.
func splitCSV(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

// durationValue parses a duration environment value with a fallback.
func durationValue(value string, fallback time.Duration) time.Duration {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(trimmed)
	if err != nil {
		return fallback
	}
	return parsed
}

// intValue parses an integer environment value with a fallback.
func intValue(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}
