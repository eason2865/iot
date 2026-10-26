package platform

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"iot/internal/contracts"
)

func NormalizeListRequest(page PageRequest, kind string) (PageRequest, error) {
	n, err := NormalizePageRequest(page.Size, page.Cursor)
	if err != nil {
		return page, err
	}
	page.Size = n.Size
	if !contracts.IsValidTenantID(page.TenantID) {
		return page, fmt.Errorf("%w: valid tenantId is required", ErrInvalidPage)
	}
	if kind == "telemetry" || page.DeviceID != "" {
		if err := contracts.ValidateDeviceIdentity(page.TenantID, page.DeviceID); err != nil {
			return page, fmt.Errorf("%w: invalid deviceId", ErrInvalidPage)
		}
	}
	if page.Sort == "" {
		page.Sort = "desc"
	}
	if page.Sort != "asc" && page.Sort != "desc" {
		return page, fmt.Errorf("%w: sort must be asc or desc", ErrInvalidPage)
	}
	page.HasStartTime = page.HasStartTime || !page.StartTime.IsZero()
	page.HasEndTime = page.HasEndTime || !page.EndTime.IsZero()
	if page.HasStartTime && page.HasEndTime && !page.StartTime.Before(page.EndTime) {
		return page, fmt.Errorf("%w: startTime must be earlier than endTime", ErrInvalidPage)
	}
	for _, stamp := range []time.Time{page.StartTime, page.EndTime} {
		if !stamp.IsZero() && (stamp.Year() < 1 || stamp.Year() > 9999 || stamp.Nanosecond()%1000000 != 0) {
			return page, fmt.Errorf("%w: timestamp outside supported range", ErrInvalidPage)
		}
	}
	if page.Type != "" {
		if err := contracts.ValidateStorageField("type", page.Type, 64); err != nil {
			return page, fmt.Errorf("%w: invalid telemetry type", ErrInvalidPage)
		}
	}
	if page.Status != "" {
		switch contracts.CommandStatus(page.Status) {
		case contracts.CommandStatusCreated, contracts.CommandStatusPublished, contracts.CommandStatusSent, contracts.CommandStatusAcked, contracts.CommandStatusFailed, contracts.CommandStatusTimeout:
		default:
			return page, fmt.Errorf("%w: invalid command status", ErrInvalidPage)
		}
	}
	return page, nil
}

type listCursor struct {
	Version int    `json:"v"`
	Scope   string `json:"scope"`
	Time    string `json:"time"`
	ID      string `json:"id"`
}

func cursorScope(page PageRequest, kind string) string {
	page.Size = 0
	page.Cursor = ""
	page.StartTime = page.StartTime.UTC()
	page.EndTime = page.EndTime.UTC()
	b, _ := json.Marshal(struct {
		Kind string
		Page PageRequest
	}{kind, page})
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func encodePageCursor(page PageRequest, kind string, at time.Time, id string) string {
	b, _ := json.Marshal(listCursor{1, cursorScope(page, kind), at.UTC().Format(time.RFC3339Nano), id})
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodePageCursor(page PageRequest, kind string) (time.Time, string, error) {
	if page.Cursor == "" {
		return time.Time{}, "", nil
	}
	if len(page.Cursor) > 4096 {
		return time.Time{}, "", errInvalidCursor
	}
	raw, err := base64.RawURLEncoding.DecodeString(page.Cursor)
	if err != nil {
		return time.Time{}, "", errInvalidCursor
	}
	var c listCursor
	if err = json.Unmarshal(raw, &c); err != nil || c.Version != 1 || c.Scope != cursorScope(page, kind) || c.ID == "" {
		return time.Time{}, "", errInvalidCursor
	}
	stamp, err := time.Parse(time.RFC3339Nano, c.Time)
	if contracts.ValidateStorageField("cursor id", c.ID, 64) != nil {
		return time.Time{}, "", errInvalidCursor
	}
	if err != nil || kind == "telemetry" && stamp.Nanosecond()%1000000 != 0 || kind == "commands" && stamp.Nanosecond()%1000 != 0 {
		return time.Time{}, "", errInvalidCursor
	}
	return stamp, c.ID, nil
}
