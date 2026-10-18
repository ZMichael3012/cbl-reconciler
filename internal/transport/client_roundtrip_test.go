package transport_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"
	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/transport"
)

type connectorStub struct {
	connectorv1.UnimplementedConnectorServiceServer
	lastRequest *connectorv1.ListGatewayPaymentsRequest
	fail        bool
}

func (s *connectorStub) ListGatewayPayments(_ context.Context, req *connectorv1.ListGatewayPaymentsRequest) (*connectorv1.ListGatewayPaymentsResponse, error) {
	s.lastRequest = req
	if s.fail {
		return nil, status.Error(codes.Unavailable, "connector down")
	}
	return &connectorv1.ListGatewayPaymentsResponse{
		Payments: []*connectorv1.CanonicalPaymentStatus{{
			PaymentId:      "01a14f70-0000-7000-8000-000000000001",
			GatewayId:      "gw-1",
			Provider:       "coingate",
			Status:         connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED,
			ReceivedAmount: &commonv1.Money{MinorUnits: 5000000, Asset: "USDT", Decimals: 6},
			RawStatus:      "paid",
		}},
		NextCursor: "x1:1",
	}, nil
}

type paymentStub struct {
	paymentv1.UnimplementedPaymentServiceServer
	empty bool
}

func (s *paymentStub) GetPayment(_ context.Context, req *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	if req.GetPaymentId() == "missing" {
		return nil, status.Error(codes.NotFound, "payment not found")
	}
	if req.GetPaymentId() == "broken" {
		return nil, status.Error(codes.Unavailable, "payment down")
	}
	if s.empty {
		return &paymentv1.GetPaymentResponse{}, nil
	}
	return &paymentv1.GetPaymentResponse{Payment: &paymentv1.Payment{
		PaymentId:      req.GetPaymentId(),
		MerchantId:     "merchant-1",
		State:          paymentv1.PaymentState_PAYMENT_STATE_COMPLETED,
		ExpectedAmount: &commonv1.Money{MinorUnits: 5000000, Asset: "USDT", Decimals: 6},
		CreatedAt:      timestamppb.New(time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)),
	}}, nil
}

type ledgerStub struct {
	ledgerv1.UnimplementedLedgerServiceServer
}

func (s *ledgerStub) ListEntries(_ context.Context, req *ledgerv1.ListEntriesRequest) (*ledgerv1.ListEntriesResponse, error) {
	if req.GetMerchantRef() == "broken" {
		return nil, status.Error(codes.Unavailable, "ledger down")
	}
	if req.GetCursor() == "" {
		return &ledgerv1.ListEntriesResponse{
			Entries:    []*ledgerv1.Entry{{EntryId: 1, RefId: "other"}},
			NextCursor: "1",
		}, nil
	}
	return &ledgerv1.ListEntriesResponse{
		Entries: []*ledgerv1.Entry{{EntryId: 2, RefId: "target-ref"}},
	}, nil
}

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

func TestConnectorClientRoundTrip(t *testing.T) {
	t.Parallel()
	stub := &connectorStub{}
	addr := startStub(t, func(server *grpc.Server) { connectorv1.RegisterConnectorServiceServer(server, stub) })
	client, err := transport.DialConnector(addr)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	from := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 18, 11, 0, 0, 0, time.UTC)
	page, err := client.ListGatewayPayments(context.Background(), "coingate", from, to, 50, "x1:50")
	require.NoError(t, err)
	require.Len(t, page.Payments, 1)
	assert.Equal(t, domain.ProviderStatusConfirmed, page.Payments[0].Status)
	assert.Equal(t, int64(5000000), page.Payments[0].ReceivedAmount.MinorUnits)
	assert.Equal(t, "x1:1", page.NextCursor)
	require.NotNil(t, stub.lastRequest)
	assert.Equal(t, uint32(50), stub.lastRequest.GetLimit())
	assert.Equal(t, "x1:50", stub.lastRequest.GetCursor())
	assert.Equal(t, from, stub.lastRequest.GetFrom().AsTime())

	stub.fail = true
	_, err = client.ListGatewayPayments(context.Background(), "coingate", from, to, 50, "")
	require.Error(t, err)
}

func TestPaymentClientRoundTrip(t *testing.T) {
	t.Parallel()
	stub := &paymentStub{}
	addr := startStub(t, func(server *grpc.Server) { paymentv1.RegisterPaymentServiceServer(server, stub) })
	client, err := transport.DialPayment(addr)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	payment, err := client.GetPayment(context.Background(), "01a14f70-0000-7000-8000-000000000001")
	require.NoError(t, err)
	assert.Equal(t, domain.PaymentStateCompleted, payment.State)
	assert.Equal(t, "merchant-1", payment.MerchantID)
	assert.Equal(t, int64(5000000), payment.ExpectedAmount.MinorUnits)
	assert.False(t, payment.CreatedAt.IsZero())

	_, err = client.GetPayment(context.Background(), "missing")
	require.ErrorIs(t, err, domain.ErrPaymentNotFound)

	_, err = client.GetPayment(context.Background(), "broken")
	require.Error(t, err)
	require.NotErrorIs(t, err, domain.ErrPaymentNotFound)

	stub.empty = true
	_, err = client.GetPayment(context.Background(), "01a14f70-0000-7000-8000-000000000002")
	require.Error(t, err, "a response without a payment is an error")
}

func TestLedgerClientHasEntryForRef(t *testing.T) {
	t.Parallel()
	addr := startStub(t, func(server *grpc.Server) { ledgerv1.RegisterLedgerServiceServer(server, &ledgerStub{}) })
	client, err := transport.DialLedger(addr, 0, 10)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	from := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 18, 23, 0, 0, 0, time.UTC)
	found, err := client.HasEntryForRef(context.Background(), "merchant-1", "target-ref", from, to)
	require.NoError(t, err)
	assert.True(t, found, "the second page carries the ref")

	found, err = client.HasEntryForRef(context.Background(), "merchant-1", "absent-ref", from, to)
	require.NoError(t, err)
	assert.False(t, found)

	_, err = client.HasEntryForRef(context.Background(), "broken", "target-ref", from, to)
	require.Error(t, err)
}
