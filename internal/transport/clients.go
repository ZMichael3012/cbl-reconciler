// Package transport contains the outbound gRPC clients of the reconciler
// (Connector, Payment, Ledger).
package transport

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/connector/v1"
	ledgerv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/ledger/v1"
	paymentv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/payment/v1"

	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// Client timeouts per call class.
const (
	// connectorTimeout matches the ListGatewayPayments RPC budget (CBL-003).
	connectorTimeout = 10 * time.Second
	// paymentTimeout matches the GetPayment budget (CBL-003).
	paymentTimeout = time.Second
	// ledgerTimeout matches the ListEntries budget (CBL-003).
	ledgerTimeout = 500 * time.Millisecond
)

// dial builds a trusted-network gRPC client connection; mTLS arrives with the
// platform task 0.6.
func dial(target string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("reconciler: dial %q: %w", target, err)
	}
	return conn, nil
}

// ConnectorClient adapts ConnectorService/ListGatewayPayments.
type ConnectorClient struct {
	conn *grpc.ClientConn
	svc  connectorv1.ConnectorServiceClient
}

// DialConnector connects the connector client.
func DialConnector(target string) (*ConnectorClient, error) {
	conn, err := dial(target)
	if err != nil {
		return nil, err
	}
	return &ConnectorClient{conn: conn, svc: connectorv1.NewConnectorServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *ConnectorClient) Close() error { return c.conn.Close() }

// ListGatewayPayments returns one export page in the domain shape.
func (c *ConnectorClient) ListGatewayPayments(ctx context.Context, provider string, from, to time.Time, limit int, cursor string) (domain.ExportPage, error) {
	callCtx, cancel := context.WithTimeout(ctx, connectorTimeout)
	defer cancel()
	response, err := c.svc.ListGatewayPayments(callCtx, &connectorv1.ListGatewayPaymentsRequest{
		Provider: provider,
		From:     timestamppb.New(from.UTC()),
		To:       timestamppb.New(to.UTC()),
		Limit:    uint32(limit),
		Cursor:   cursor,
	})
	if err != nil {
		return domain.ExportPage{}, fmt.Errorf("reconciler: list gateway payments: %w", err)
	}
	page := domain.ExportPage{NextCursor: response.GetNextCursor()}
	for _, row := range response.GetPayments() {
		page.Payments = append(page.Payments, GatewayPaymentFromProto(row))
	}
	return page, nil
}

// PaymentClient adapts PaymentService/GetPayment.
type PaymentClient struct {
	conn *grpc.ClientConn
	svc  paymentv1.PaymentServiceClient
}

// DialPayment connects the payment client.
func DialPayment(target string) (*PaymentClient, error) {
	conn, err := dial(target)
	if err != nil {
		return nil, err
	}
	return &PaymentClient{conn: conn, svc: paymentv1.NewPaymentServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *PaymentClient) Close() error { return c.conn.Close() }

// GetPayment returns the payment summary; a NotFound code maps to
// domain.ErrPaymentNotFound.
func (c *PaymentClient) GetPayment(ctx context.Context, paymentID string) (domain.PaymentSummary, error) {
	callCtx, cancel := context.WithTimeout(ctx, paymentTimeout)
	defer cancel()
	response, err := c.svc.GetPayment(callCtx, &paymentv1.GetPaymentRequest{PaymentId: paymentID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return domain.PaymentSummary{}, domain.ErrPaymentNotFound
		}
		return domain.PaymentSummary{}, fmt.Errorf("reconciler: get payment: %w", err)
	}
	payment := response.GetPayment()
	if payment == nil {
		return domain.PaymentSummary{}, fmt.Errorf("reconciler: get payment returned no payment")
	}
	summary := domain.PaymentSummary{
		PaymentID:      payment.GetPaymentId(),
		MerchantID:     payment.GetMerchantId(),
		Provider:       payment.GetProvider(),
		State:          PaymentStateFromProto(payment.GetState()),
		ExpectedAmount: moneyFromProto(payment.GetExpectedAmount()),
		ReceivedAmount: moneyFromProto(payment.GetReceivedAmount()),
	}
	if payment.GetCreatedAt() != nil {
		summary.CreatedAt = payment.GetCreatedAt().AsTime().UTC()
	}
	return summary, nil
}

// LedgerClient adapts LedgerService/ListEntries for evidence checks.
type LedgerClient struct {
	conn     *grpc.ClientConn
	svc      ledgerv1.LedgerServiceClient
	pageSize uint32
	maxPages int
}

// DialLedger connects the ledger client; the scan is bounded by maxPages
// pages of pageSize entries.
func DialLedger(target string, pageSize uint32, maxPages int) (*LedgerClient, error) {
	if pageSize == 0 {
		pageSize = 200
	}
	if maxPages <= 0 {
		maxPages = 10
	}
	conn, err := dial(target)
	if err != nil {
		return nil, err
	}
	return &LedgerClient{conn: conn, svc: ledgerv1.NewLedgerServiceClient(conn), pageSize: pageSize, maxPages: maxPages}, nil
}

// Close closes the connection.
func (c *LedgerClient) Close() error { return c.conn.Close() }

// HasEntryForRef pages the merchant statement of the period and reports
// whether an entry carries the ref id; the scan is bounded.
func (c *LedgerClient) HasEntryForRef(ctx context.Context, merchantRef, refID string, from, to time.Time) (bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, ledgerTimeout)
	defer cancel()
	periodFrom := timestamppb.New(from.UTC())
	periodTo := timestamppb.New(to.UTC())
	cursor := ""
	for page := 0; page < c.maxPages; page++ {
		response, err := c.svc.ListEntries(callCtx, &ledgerv1.ListEntriesRequest{
			MerchantRef: merchantRef,
			Limit:       c.pageSize,
			Cursor:      cursor,
			PeriodFrom:  periodFrom,
			PeriodTo:    periodTo,
		})
		if err != nil {
			return false, fmt.Errorf("reconciler: list ledger entries: %w", err)
		}
		for _, entry := range response.GetEntries() {
			if entry.GetRefId() == refID {
				return true, nil
			}
		}
		cursor = response.GetNextCursor()
		if cursor == "" {
			return false, nil
		}
	}
	return false, nil
}

// GatewayPaymentFromProto maps one canonical export row to the domain shape;
// an UNSPECIFIED status with a raw word is the unmapped evidence case, the
// pending status maps to the pre-deposit state.
func GatewayPaymentFromProto(row *connectorv1.CanonicalPaymentStatus) domain.GatewayPayment {
	payment := domain.GatewayPayment{
		PaymentID:     row.GetPaymentId(),
		GatewayID:     row.GetGatewayId(),
		Provider:      row.GetProvider(),
		Status:        ProviderStatusFromProto(row.GetStatus()),
		RawStatus:     row.GetRawStatus(),
		Confirmations: row.GetConfirmations(),
		Network:       row.GetNetwork(),
		TXID:          row.GetTxid(),
	}
	if row.GetReceivedAmount() != nil {
		payment.ReceivedAmount = moneyFromProto(row.GetReceivedAmount())
	}
	if row.GetGatewayEventTs() != nil {
		payment.GatewayEventTS = row.GetGatewayEventTs().AsTime().UTC()
	}
	return payment
}

// ProviderStatusFromProto maps the contract enum to the domain status; the
// unspecified value means "no canonical status" (raw only).
func ProviderStatusFromProto(value connectorv1.CanonicalPaymentStatusValue) domain.ProviderStatus {
	switch value {
	case connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_PENDING:
		return domain.ProviderStatusPending
	case connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_DETECTED:
		return domain.ProviderStatusDetected
	case connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED:
		return domain.ProviderStatusConfirmed
	case connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_FAILED:
		return domain.ProviderStatusFailed
	case connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_EXPIRED:
		return domain.ProviderStatusExpired
	case connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_UNDERPAID:
		return domain.ProviderStatusUnderpaid
	case connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_OVERPAID:
		return domain.ProviderStatusOverpaid
	default:
		return ""
	}
}

// PaymentStateFromProto maps the payment enum to the domain state.
func PaymentStateFromProto(value paymentv1.PaymentState) domain.PaymentState {
	switch value {
	case paymentv1.PaymentState_PAYMENT_STATE_CREATING:
		return domain.PaymentStateCreating
	case paymentv1.PaymentState_PAYMENT_STATE_CREATED:
		return domain.PaymentStateCreated
	case paymentv1.PaymentState_PAYMENT_STATE_PENDING:
		return domain.PaymentStatePending
	case paymentv1.PaymentState_PAYMENT_STATE_DETECTED:
		return domain.PaymentStateDetected
	case paymentv1.PaymentState_PAYMENT_STATE_CONFIRMING:
		return domain.PaymentStateConfirming
	case paymentv1.PaymentState_PAYMENT_STATE_COMPLETED:
		return domain.PaymentStateCompleted
	case paymentv1.PaymentState_PAYMENT_STATE_EXPIRED:
		return domain.PaymentStateExpired
	case paymentv1.PaymentState_PAYMENT_STATE_UNDERPAID:
		return domain.PaymentStateUnderpaid
	case paymentv1.PaymentState_PAYMENT_STATE_OVERPAID:
		return domain.PaymentStateOverpaid
	case paymentv1.PaymentState_PAYMENT_STATE_FAILED:
		return domain.PaymentStateFailed
	case paymentv1.PaymentState_PAYMENT_STATE_CANCELLED:
		return domain.PaymentStateCancelled
	default:
		return domain.PaymentState("")
	}
}

// moneyFromProto converts the contract money to the kit value.
func moneyFromProto(value *commonv1.Money) money.Money {
	if value == nil {
		return money.Money{}
	}
	return money.Money{
		MinorUnits: value.GetMinorUnits(),
		Asset:      value.GetAsset(),
		Decimals:   value.GetDecimals(),
	}
}
