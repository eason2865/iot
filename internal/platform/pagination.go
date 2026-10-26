package platform

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrInvalidPage = errors.New("invalid pagination")

var errInvalidCursor = fmt.Errorf("%w: invalid cursor", ErrInvalidPage)

const (
	defaultPageSize = 50
	maxPageSize     = 100
)

// PageRequest implements opaque, keyset-based pagination. Offset pagination
// becomes increasingly expensive as an IoT tenant accumulates records.
type PageRequest struct {
	Size         int
	Cursor       string
	TenantID     string
	DeviceID     string
	StartTime    time.Time
	HasStartTime bool
	EndTime      time.Time
	HasEndTime   bool
	Type         string
	Status       string
	Sort         string
}

func NormalizePageRequest(size int, cursor string) (PageRequest, error) {
	if len(cursor) > 4096 {
		return PageRequest{}, errInvalidCursor
	}
	if size == 0 {
		size = defaultPageSize
	}
	if size < 1 || size > maxPageSize {
		return PageRequest{}, fmt.Errorf("%w: pageSize must be between 1 and %d", ErrInvalidPage, maxPageSize)
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
	if err != nil || !utf8.Valid(decoded) {
		return nil, errInvalidCursor
	}
	parts := strings.Split(string(decoded), "\x00")
	if len(parts) != count {
		return nil, errInvalidCursor
	}
	for _, part := range parts {
		if part == "" {
			return nil, errInvalidCursor
		}
	}
	return parts, nil
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
