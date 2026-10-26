package platform

import (
	"context"
	"sort"
	"time"
)

func (s *memoryStore) ListDevicesPageContext(ctx context.Context, page PageRequest) ([]Device, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	page, err := NormalizeListRequest(page, "devices")
	if err != nil {
		return nil, "", err
	}
	tenant, after, err := decodeDeviceCursor(page.Cursor)
	if err != nil || tenant != "" && tenant != page.TenantID {
		return nil, "", errInvalidCursor
	}
	var rows []Device
	for _, d := range s.ListDevices() {
		if d.TenantID == page.TenantID && d.DeviceID > after {
			d.Secret = ""
			rows = append(rows, d)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].DeviceID < rows[j].DeviceID })
	var out []Device
	var budget responsePageBudget
	for _, d := range rows {
		ok, err := budget.accept(deviceListBytes(d), len(out), page.Size)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			last := out[len(out)-1]
			return out, encodeDeviceCursor(last.TenantID, last.DeviceID), nil
		}
		out = append(out, d)
	}
	return out, "", nil
}

func memoryPage[T any](page PageRequest, kind string, rows []T, key func(T) (time.Time, string), size func(T) int) ([]T, string, error) {
	at, id, err := decodePageCursor(page, kind)
	if err != nil {
		return nil, "", err
	}
	less := func(a, b T) bool {
		ta, ia := key(a)
		tb, ib := key(b)
		if ta.Equal(tb) {
			if page.Sort == "asc" {
				return ia < ib
			}
			return ia > ib
		}
		if page.Sort == "asc" {
			return ta.Before(tb)
		}
		return ta.After(tb)
	}
	sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })
	var out []T
	var budget responsePageBudget
	for _, row := range rows {
		stamp, rid := key(row)
		if page.Cursor != "" {
			before := stamp.Before(at) || stamp.Equal(at) && rid < id
			after := stamp.After(at) || stamp.Equal(at) && rid > id
			if page.Sort == "asc" && !after || page.Sort == "desc" && !before {
				continue
			}
		}
		ok, err := budget.accept(size(row), len(out), page.Size)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			t, id := key(out[len(out)-1])
			return out, encodePageCursor(page, kind, t, id), nil
		}
		out = append(out, row)
	}
	return out, "", nil
}

func (s *memoryStore) ListCommandsPageContext(ctx context.Context, page PageRequest) ([]Command, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	page, err := NormalizeListRequest(page, "commands")
	if err != nil {
		return nil, "", err
	}
	var rows []Command
	for _, c := range s.ListCommands() {
		if c.TenantID != page.TenantID || page.DeviceID != "" && c.DeviceID != page.DeviceID || page.Status != "" && string(c.Status) != page.Status || page.HasStartTime && c.CreatedAt.Before(page.StartTime) || page.HasEndTime && !c.CreatedAt.Before(page.EndTime) {
			continue
		}
		rows = append(rows, c)
	}
	return memoryPage(page, "commands", rows, func(c Command) (time.Time, string) { return c.CreatedAt, c.ID }, commandListBytes)
}
func (s *memoryStore) ListTelemetryPageContext(ctx context.Context, page PageRequest) ([]TelemetryRecord, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	page, err := NormalizeListRequest(page, "telemetry")
	if err != nil {
		return nil, "", err
	}
	var rows []TelemetryRecord
	for _, r := range s.ListTelemetry(page.TenantID, page.DeviceID) {
		stamp := time.UnixMilli(r.Ts)
		if page.Type != "" && r.Type != page.Type || page.HasStartTime && stamp.Before(page.StartTime) || page.HasEndTime && !stamp.Before(page.EndTime) {
			continue
		}
		rows = append(rows, r)
	}
	return memoryPage(page, "telemetry", rows, func(r TelemetryRecord) (time.Time, string) { return time.UnixMilli(r.Ts), r.MsgID }, telemetryListBytes)
}
