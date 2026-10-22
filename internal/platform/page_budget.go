package platform

import "errors"

// Leave room below gRPC's default 4 MiB ceiling for the response and cursor.
// Each entry is charged its stored text/bytes plus a conservative protobuf
// metadata allowance. Iterate rows instead of retaining an entire count page.
const MaxListResponseBytes = 2 << 20
const listContentBytes = MaxListResponseBytes - 4096

var ErrListItemTooLarge = errors.New("stored record exceeds the list response budget; operator repair is required")

type responsePageBudget struct{ used int }

func (b *responsePageBudget) accept(bytes, count, limit int) (bool, error) {
	if count >= limit {
		return false, nil
	}
	if bytes > listContentBytes-b.used {
		if count == 0 {
			return false, ErrListItemTooLarge
		}
		return false, nil
	}
	b.used += bytes
	return true, nil
}
func tenantListBytes(t Tenant) int { return 512 + len(t.ID) + len(t.Name) }
func deviceListBytes(d Device) int { return 512 + len(d.TenantID) + len(d.DeviceID) + len(d.ProductID) }
func commandListBytes(c Command) int {
	return 512 + len(c.ID) + len(c.TenantID) + len(c.DeviceID) + len(c.Status) + len(c.Payload)
}
func telemetryListBytes(t TelemetryRecord) int {
	return 512 + len(t.MsgID) + len(t.TenantID) + len(t.DeviceID) + len(t.Type) + len(t.Version) + len(t.Payload)
}
