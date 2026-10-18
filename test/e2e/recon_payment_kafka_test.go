//go:build e2e

package e2e

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"
	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-reconciler/internal/storage"
	"github.com/cbl/cbl-reconciler/internal/transport"
	"github.com/cbl/cbl-reconciler/internal/usecase"
)

// Scenario entities.
const (
	paymentLostWebhook = "01a14f70-0000-7000-8000-000000000601"
	paymentExpired     = "01a14f70-0000-7000-8000-000000000602"
	paymentMissingLedg = "01a14f70-0000-7000-8000-000000000603"
	paymentDupFirst    = "01a14f70-0000-7000-8000-000000000604"
	paymentDupSecond   = "01a14f70-0000-7000-8000-000000000605"
	paymentSeenEvent   = "01a14f70-0000-7000-8000-000000000606"
	testMerchant       = "01a14f70-0000-7000-8000-0000000006aa"
	testTXID           = "0xduplicate-e2e"
)

func setupPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("recon_e2e"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func setupKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("cbl-reconciler-e2e"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	topics := []string{
		usecase.TopicGatewayEventsCanonical,
		usecase.TopicGatewayEventsCanonical + inbox.RetrySuffix5s,
		usecase.TopicGatewayPayoutEvents,
		usecase.TopicReconAlerts,
	}
	for _, topic := range topics {
		_, err := admin.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err, topic)
	}
	return brokers
}

// connectorStub serves a static export page.
type connectorStub struct {
	connectorv1.UnimplementedConnectorServiceServer
	page *connectorv1.ListGatewayPaymentsResponse
}

func (s *connectorStub) ListGatewayPayments(context.Context, *connectorv1.ListGatewayPaymentsRequest) (*connectorv1.ListGatewayPaymentsResponse, error) {
	return s.page, nil
}

// paymentStub serves the payment projections of the scenario.
type paymentStub struct {
	paymentv1.UnimplementedPaymentServiceServer
	payments map[string]*paymentv1.Payment
}

func (s *paymentStub) GetPayment(_ context.Context, req *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	payment, ok := s.payments[req.GetPaymentId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "payment not found")
	}
	return &paymentv1.GetPaymentResponse{Payment: payment}, nil
}

// ledgerStub serves a statement with entries for the configured refs.
type ledgerStub struct {
	ledgerv1.UnimplementedLedgerServiceServer
	refs []string
}

func (s *ledgerStub) ListEntries(_ context.Context, req *ledgerv1.ListEntriesRequest) (*ledgerv1.ListEntriesResponse, error) {
	response := &ledgerv1.ListEntriesResponse{}
	for i, ref := range s.refs {
		response.Entries = append(response.Entries, &ledgerv1.Entry{
			EntryId:   int64(i + 1),
			AccountId: req.GetMerchantRef(),
			RefType:   "payment",
			RefId:     ref,
		})
	}
	return response, nil
}

// startStub starts a gRPC server with the given registrations.
func startStub(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// paymentMsg builds a payment projection.
func paymentMsg(paymentID string, state paymentv1.PaymentState, received int64) *paymentv1.Payment {
	payment := &paymentv1.Payment{
		PaymentId:      paymentID,
		MerchantId:     testMerchant,
		State:          state,
		ExpectedAmount: &commonv1.Money{MinorUnits: 5_000_000, Asset: "USDT", Decimals: 6},
		CreatedAt:      timestamppb.New(time.Now().UTC().Add(-2 * time.Hour)),
	}
	if received > 0 {
		payment.ReceivedAmount = &commonv1.Money{MinorUnits: received, Asset: "USDT", Decimals: 6}
	}
	return payment
}

// exportRow builds one canonical export row.
func exportRow(paymentID, txid string) *connectorv1.CanonicalPaymentStatus {
	return &connectorv1.CanonicalPaymentStatus{
		PaymentId:      paymentID,
		GatewayId:      "gw-" + paymentID[len(paymentID)-4:],
		Provider:       "coingate",
		Status:         connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED,
		RawStatus:      "paid",
		ReceivedAmount: &commonv1.Money{MinorUnits: 5_000_000, Asset: "USDT", Decimals: 6},
		Network:        "usdt_trc20",
		Txid:           txid,
		GatewayEventTs: timestamppb.New(time.Now().UTC().Add(-time.Hour)),
	}
}

// pollMaybe returns the next record or nil when nothing arrives in the timeout.
func pollMaybe(t *testing.T, consumer *kgo.Client, timeout time.Duration) *kgo.Record {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := consumer.PollFetches(pollCtx)
		cancel()
		var found *kgo.Record
		fetches.EachRecord(func(record *kgo.Record) {
			if found == nil {
				found = record
			}
		})
		if found != nil {
			return found
		}
	}
	return nil
}

func TestE2EReconPaymentContour(t *testing.T) {
	db := setupPostgres(t)
	brokers := setupKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	log := zap.NewNop()

	connectorAddr := startStub(t, func(server *grpc.Server) {
		connectorv1.RegisterConnectorServiceServer(server, &connectorStub{
			page: &connectorv1.ListGatewayPaymentsResponse{
				Payments: []*connectorv1.CanonicalPaymentStatus{
					exportRow(paymentLostWebhook, "0xlost"),
					exportRow(paymentExpired, "0xexpired"),
					exportRow(paymentMissingLedg, "0xmissing"),
					exportRow(paymentDupFirst, testTXID),
					exportRow(paymentDupSecond, testTXID),
					exportRow(paymentSeenEvent, "0xseen"),
				},
			},
		})
	})
	paymentAddr := startStub(t, func(server *grpc.Server) {
		paymentv1.RegisterPaymentServiceServer(server, &paymentStub{payments: map[string]*paymentv1.Payment{
			paymentLostWebhook: paymentMsg(paymentLostWebhook, paymentv1.PaymentState_PAYMENT_STATE_PENDING, 0),
			paymentExpired:     paymentMsg(paymentExpired, paymentv1.PaymentState_PAYMENT_STATE_EXPIRED, 0),
			paymentMissingLedg: paymentMsg(paymentMissingLedg, paymentv1.PaymentState_PAYMENT_STATE_COMPLETED, 5_000_000),
			paymentDupFirst:    paymentMsg(paymentDupFirst, paymentv1.PaymentState_PAYMENT_STATE_COMPLETED, 5_000_000),
			paymentDupSecond:   paymentMsg(paymentDupSecond, paymentv1.PaymentState_PAYMENT_STATE_COMPLETED, 5_000_000),
			paymentSeenEvent:   paymentMsg(paymentSeenEvent, paymentv1.PaymentState_PAYMENT_STATE_PENDING, 0),
		}})
	})
	ledgerAddr := startStub(t, func(server *grpc.Server) {
		ledgerv1.RegisterLedgerServiceServer(server, &ledgerStub{refs: []string{
			paymentLostWebhook, paymentExpired, paymentDupFirst, paymentDupSecond, paymentSeenEvent,
		}})
	})

	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()
	relay := outbox.NewRelay(db, publisher, log, outbox.WithInterval(50*time.Millisecond))
	go func() { _ = relay.Run(ctx) }()

	store := storage.NewObservationRepository(db)
	canonicalGroup := "cbl-reconciler-canonical-e2e"
	canonicalProcessor := inbox.NewProcessor(canonicalGroup, db, inbox.NewGuard(),
		usecase.NewCanonicalHandler(store, log), publisher,
		inbox.NewRouter(usecase.TopicGatewayEventsCanonical, 3), log)
	canonicalRunner, err := kafkaconsumer.New(brokers, canonicalGroup,
		[]string{usecase.TopicGatewayEventsCanonical}, canonicalProcessor, log)
	require.NoError(t, err)
	defer canonicalRunner.Close()
	go func() { _ = canonicalRunner.Run(ctx) }()

	payoutGroup := "cbl-reconciler-payouts-e2e"
	payoutProcessor := inbox.NewProcessor(payoutGroup, db, inbox.NewGuard(),
		usecase.NewPayoutHandler(store, log), publisher,
		inbox.NewRouter(usecase.TopicGatewayPayoutEvents, 3), log)
	payoutRunner, err := kafkaconsumer.New(brokers, payoutGroup,
		[]string{usecase.TopicGatewayPayoutEvents}, payoutProcessor, log)
	require.NoError(t, err)
	defer payoutRunner.Close()
	go func() { _ = payoutRunner.Run(ctx) }()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	// A canonical event for paymentSeenEvent is consumed before the pass: the
	// observation suppresses the replay for that row (FR-5).
	seenPayload, err := proto.Marshal(&eventsv1.CanonicalPaymentEvent{
		PaymentId: paymentSeenEvent,
		GatewayId: "gw-0606",
		Provider:  "coingate",
		Type:      eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED,
		RawStatus: "paid",
	})
	require.NoError(t, err)
	eventID := uuid.Must(uuid.NewV7()).String()
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{
		Topic: usecase.TopicGatewayEventsCanonical,
		Key:   []byte(paymentSeenEvent),
		Value: seenPayload,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(eventID)},
			{Key: outbox.HeaderEventType, Value: []byte("gateway.payment.confirmed")},
		},
	}).FirstErr())
	require.Eventually(t, func() bool {
		found, seenErr := store.Seen(ctx, paymentSeenEvent, "confirmed")
		return seenErr == nil && found
	}, 30*time.Second, 200*time.Millisecond, "the canonical observation must be recorded")

	// The payout verdict observation path (FR-5 for gateway.payout.events).
	payoutID := "01a14f70-0000-7000-8000-000000000651"
	payoutPayload, err := proto.Marshal(&eventsv1.GatewayPayoutEvent{
		PayoutId:        payoutID,
		GatewayPayoutId: "po-e2e",
		Provider:        "coingate",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	})
	require.NoError(t, err)
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{
		Topic: usecase.TopicGatewayPayoutEvents,
		Key:   []byte(payoutID),
		Value: payoutPayload,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(uuid.Must(uuid.NewV7()).String())},
			{Key: outbox.HeaderEventType, Value: []byte("gateway.payout.confirmed")},
		},
	}).FirstErr())
	require.Eventually(t, func() bool {
		var count int64
		require.NoError(t, db.Table("recon_payout_observations").Where("payout_id = ?", payoutID).Count(&count).Error)
		return count == 1
	}, 30*time.Second, 200*time.Millisecond, "the payout observation must be recorded")

	connectorClient, err := transport.DialConnector(connectorAddr)
	require.NoError(t, err)
	defer func() { _ = connectorClient.Close() }()
	paymentClient, err := transport.DialPayment(paymentAddr)
	require.NoError(t, err)
	defer func() { _ = paymentClient.Close() }()
	ledgerClient, err := transport.DialLedger(ledgerAddr, 0, 10)
	require.NoError(t, err)
	defer func() { _ = ledgerClient.Close() }()

	pass, err := usecase.NewReconPass(connectorClient, paymentClient, ledgerClient,
		storage.NewAlertStore(db), storage.NewReplayStore(db), store, storage.NewRunRepository(db),
		usecase.Config{Providers: []string{"coingate"}, PageLimit: 100, Lookback: 24 * time.Hour, Log: log})
	require.NoError(t, err)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(usecase.TopicReconAlerts, usecase.TopicGatewayEventsCanonical),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	report, err := pass.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.Providers, 1)
	assert.Equal(t, 1, report.Providers[0].Replayed, "only the lost webhook row replays")
	assert.Equal(t, 3, report.Providers[0].Alerts, "the terminal mismatch, the missing ledger and the duplicate txid alert")

	// The three alerts and the replay reach their topics; the seen row never
	// replays.
	alerts := map[string]*eventsv1.ReconAlert{}
	replaySeen := false
	deadline := time.Now().Add(60 * time.Second)
	for (len(alerts) < 3 || !replaySeen) && time.Now().Before(deadline) {
		record := pollMaybe(t, consumer, 5*time.Second)
		if record == nil {
			continue
		}
		switch record.Topic {
		case usecase.TopicReconAlerts:
			var alert eventsv1.ReconAlert
			require.NoError(t, proto.Unmarshal(record.Value, &alert))
			alerts[alert.GetRefId()] = &alert
		case usecase.TopicGatewayEventsCanonical:
			var event eventsv1.CanonicalPaymentEvent
			require.NoError(t, proto.Unmarshal(record.Value, &event))
			if event.GetPaymentId() == paymentLostWebhook {
				assert.Equal(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, event.GetType())
				assert.Equal(t, int64(5_000_000), event.GetReceivedAmount().GetMinorUnits())
				replaySeen = true
			}
		}
	}
	require.Len(t, alerts, 3, "expected three recon.alerts records")
	require.True(t, replaySeen, "the lost webhook must be replayed to gateway.events.canonical")
	require.NotNil(t, alerts[paymentExpired])
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_STATUS_MISMATCH, alerts[paymentExpired].GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL, alerts[paymentExpired].GetSeverity())
	require.NotNil(t, alerts[paymentMissingLedg])
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_MISSING_IN_LEDGER, alerts[paymentMissingLedg].GetType())
	assert.Equal(t, testMerchant, alerts[paymentMissingLedg].GetMerchantId())
	require.NotNil(t, alerts[paymentDupSecond])
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_AMOUNT_MISMATCH, alerts[paymentDupSecond].GetType())

	var replays int64
	require.NoError(t, db.Table("recon_replays").Count(&replays).Error)
	assert.Equal(t, int64(1), replays, "the seen row and the duplicate rows never replay")

	// The second pass dedups: no new alert records, no new replays.
	report, err = pass.RunOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, report.Providers[0].Alerts)
	assert.Zero(t, report.Providers[0].Replayed)
	assert.Nil(t, pollMaybe(t, consumer, 3*time.Second), "the second pass publishes nothing")

	var occurrences int
	require.NoError(t, db.Raw("SELECT COALESCE(MAX(occurrences), 0) FROM recon_alert_keys").Scan(&occurrences).Error)
	assert.GreaterOrEqual(t, occurrences, 2, "the repeated detections bump the counter")

	var runs int64
	require.NoError(t, db.Table("recon_runs").Where("provider = ?", "coingate").Count(&runs).Error)
	assert.Equal(t, int64(1), runs)
}
