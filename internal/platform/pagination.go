package platform

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPageSize = 50
	maxPageSize     = 100
)

// PageRequest implements opaque, keyset-based pagination. Offset pagination
// becomes increasingly expensive as an IoT tenant accumulates records.
type PageRequest struct {
	Size     int
	Cursor   string
	TenantID string
	DeviceID string
}

func NormalizePageRequest(size int, cursor string) (PageRequest, error) {
	if size == 0 {
		size = defaultPageSize
	}
	if size < 1 || size > maxPageSize {
		return PageRequest{}, fmt.Errorf("pageSize must be between 1 and %d", maxPageSize)
	}
	return PageRequest{Size: size, Cursor: cursor}, nil
}

func encodeCursor(parts ...string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, "\x00")))
}

func decodeCursor(cursor string, count int) ([]string, error) {
	if cursor == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	parts := strings.Split(string(decoded), "\x00")
	if len(parts) != count {
		return nil, fmt.Errorf("invalid cursor")
	}
	return parts, nil
}

func encodeCommandCursor(createdAt time.Time, id string) string {
	return encodeCursor(strconv.FormatInt(createdAt.UTC().UnixNano(), 10), id)
}

func decodeCommandCursor(cursor string) (time.Time, string, error) {
	parts, err := decodeCursor(cursor, 2)
	if err != nil || parts == nil {
		return time.Time{}, "", err
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	return time.Unix(0, nanos).UTC(), parts[1], nil
}

// Device pagination uses a (tenant_id, device_id) keyset cursor.
func encodeDeviceCursor(tenantID, deviceID string) string {
	return encodeCursor(tenantID, deviceID)
}

func decodeDeviceCursor(cursor string) (string, string, error) {
	parts, err := decodeCursor(cursor, 2)
	if err != nil || parts == nil {
		return "", "", err
	}
	return parts[0], parts[1], nil
}

// Telemetry pagination uses a (received_at, msg_id) keyset cursor.
func encodeTelemetryCursor(receivedAt time.Time, msgID string) string {
	return encodeCursor(strconv.FormatInt(receivedAt.UTC().UnixNano(), 10), msgID)
}

func decodeTelemetryCursor(cursor string) (time.Time, string, error) {
	parts, err := decodeCursor(cursor, 2)
	if err != nil || parts == nil {
		return time.Time{}, "", err
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	return time.Unix(0, nanos).UTC(), parts[1], nil
}
