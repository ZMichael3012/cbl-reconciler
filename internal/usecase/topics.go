// Package usecase contains the reconciliation pass and the Kafka handlers of
// the reconciler service (17.13).
package usecase

// Topic and producer identities of the reconciler contours.
const (
	// TopicGatewayEventsCanonical is the canonical payment topic (consumed and
	// replayed to).
	TopicGatewayEventsCanonical = "gateway.events.canonical"
	// TopicGatewayPayoutEvents is the provider payout verdict topic (consumed).
	TopicGatewayPayoutEvents = "gateway.payout.events"
	// TopicReconAlerts is the produced discrepancies topic.
	TopicReconAlerts = "recon.alerts"
	// Producer is the envelope producer id of the service.
	Producer = "cbl-reconciler"
	// EventTypeReconAlert is the recon.alerts envelope event type.
	EventTypeReconAlert = "recon.alert"
	// ConsumerGroupCanonical is the default consumer group of the canonical
	// consumer.
	ConsumerGroupCanonical = "cbl-reconciler-canonical"
	// ConsumerGroupPayouts is the default consumer group of the payout event
	// consumer.
	ConsumerGroupPayouts = "cbl-reconciler-payouts"
	// SchemaVersion is the envelope schema version header value.
	SchemaVersion = "v1"
)
