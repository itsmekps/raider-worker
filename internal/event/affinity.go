package event

import "encoding/json"

// AffinityKey returns the routing key used for in-process ordering. Events
// sharing the same key are always processed by the same worker lane,
// preserving per-entity ordering (case lifecycle, assignments, payments,
// visits). If the payload carries a caseId, ordering is scoped to
// tenant+case; otherwise it falls back to tenant-level ordering, which is
// always safe since the producer already partitions Kafka by tenantId.
func (e Event) AffinityKey() string {
	if caseID := extractCaseID(e.Data); caseID != "" {
		return e.TenantID + ":" + caseID
	}
	return e.TenantID
}

func extractCaseID(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}
	var probe struct {
		CaseID string `json:"caseId"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return ""
	}
	return probe.CaseID
}
