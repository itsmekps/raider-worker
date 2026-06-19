package processor

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"github.com/raider/worker/internal/event"
	"github.com/raider/worker/internal/logger"
)

// NotificationTriggeredData is the expected shape of data for NOTIFICATION_TRIGGERED events.
type NotificationTriggeredData struct {
	NotificationID string `json:"notificationId"`
	RecipientID    string `json:"recipientId"`
	Channel        string `json:"channel"` // e.g. "sms", "email", "push"
	TemplateID     string `json:"templateId"`
	Payload        any    `json:"payload"`
}

// NotificationTriggeredProcessor handles NOTIFICATION_TRIGGERED v1 events.
type NotificationTriggeredProcessor struct {
	// inject services here, e.g.:
	// notificationService service.NotificationService
}

func NewNotificationTriggeredProcessor() *NotificationTriggeredProcessor {
	return &NotificationTriggeredProcessor{}
}

func (p *NotificationTriggeredProcessor) Process(ctx context.Context, e event.Event) error {
	log := logger.With(
		zap.String("eventType", e.EventType),
		zap.String("eventId", e.EventID),
		zap.String("tenantId", e.TenantID),
	)

	var data NotificationTriggeredData
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return &event.ErrBusinessValidation{Reason: "invalid NOTIFICATION_TRIGGERED data: " + err.Error()}
	}

	log.Info("processing NOTIFICATION_TRIGGERED",
		zap.String("notificationId", data.NotificationID),
		zap.String("recipientId", data.RecipientID),
		zap.String("channel", data.Channel),
		zap.String("templateId", data.TemplateID),
	)

	// TODO: wire real business logic here, e.g.:
	// return p.notificationService.Send(ctx, e.TenantID, data)

	log.Info("NOTIFICATION_TRIGGERED processed successfully")
	return nil
}
