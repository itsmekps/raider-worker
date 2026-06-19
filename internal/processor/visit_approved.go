package processor

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"github.com/raider/worker/internal/event"
	"github.com/raider/worker/internal/logger"
)

// VisitApprovedData is the expected shape of the data field for VISIT_APPROVED events.
type VisitApprovedData struct {
	VisitID    string `json:"visitId"`
	AgentID    string `json:"agentId"`
	CaseID     string `json:"caseId"`
	ApprovedAt string `json:"approvedAt"`
}

// VisitApprovedProcessor handles VISIT_APPROVED v1 events.
type VisitApprovedProcessor struct {
	// inject services here, e.g.:
	// visitService service.VisitService
}

func NewVisitApprovedProcessor() *VisitApprovedProcessor {
	return &VisitApprovedProcessor{}
}

func (p *VisitApprovedProcessor) Process(ctx context.Context, e event.Event) error {
	log := logger.With(
		zap.String("eventType", e.EventType),
		zap.String("eventId", e.EventID),
		zap.String("tenantId", e.TenantID),
	)

	var data VisitApprovedData
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return &event.ErrBusinessValidation{Reason: "invalid VISIT_APPROVED data: " + err.Error()}
	}

	log.Info("processing VISIT_APPROVED",
		zap.String("visitId", data.VisitID),
		zap.String("agentId", data.AgentID),
		zap.String("caseId", data.CaseID),
	)

	// TODO: wire real business logic here, e.g.:
	// return p.visitService.ApproveVisit(ctx, e.TenantID, data)

	log.Info("VISIT_APPROVED processed successfully")
	return nil
}
