package event

import (
	"encoding/json"
	"fmt"
	"time"
)

type rawEnvelope struct {
	Version   int             `json:"version"`
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	TenantID  string          `json:"tenantId"`
	Timestamp string          `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// ParseAndValidate deserialises the raw bytes and validates all mandatory fields.
// Returns ErrInvalidEnvelope (non-retriable) if the message is malformed.
func ParseAndValidate(raw RawMessage) (Event, error) {
	var env rawEnvelope
	if err := json.Unmarshal(raw.Value, &env); err != nil {
		return Event{}, &ErrInvalidEnvelope{Reason: fmt.Sprintf("json unmarshal: %v", err)}
	}
	if err := validateMandatoryFields(env); err != nil {
		return Event{}, err
	}

	ts, err := time.Parse(time.RFC3339, env.Timestamp)
	if err != nil {
		ts, err = time.Parse("2006-01-02T15:04:05Z", env.Timestamp)
		if err != nil {
			return Event{}, &ErrInvalidEnvelope{Reason: fmt.Sprintf("invalid timestamp: %s", env.Timestamp)}
		}
	}

	return Event{
		Version:   env.Version,
		EventID:   env.EventID,
		EventType: env.EventType,
		TenantID:  env.TenantID,
		Timestamp: ts,
		Data:      env.Data,
		Topic:     raw.Topic,
		Partition: raw.Partition,
		Offset:    raw.Offset,
	}, nil
}

func validateMandatoryFields(env rawEnvelope) error {
	if env.Version == 0 {
		return &ErrInvalidEnvelope{Reason: "missing or zero version"}
	}
	if env.EventID == "" {
		return &ErrInvalidEnvelope{Reason: "missing eventId"}
	}
	if env.EventType == "" {
		return &ErrInvalidEnvelope{Reason: "missing eventType"}
	}
	if env.TenantID == "" {
		return &ErrInvalidEnvelope{Reason: "missing tenantId"}
	}
	if env.Timestamp == "" {
		return &ErrInvalidEnvelope{Reason: "missing timestamp"}
	}
	if env.Data == nil {
		return &ErrInvalidEnvelope{Reason: "missing data"}
	}
	return nil
}
