package contracts

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidTopicPart = errors.New("invalid topic part")

func BuildDeviceTopic(tenantID, deviceID, suffix string) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	deviceID = strings.TrimSpace(deviceID)
	suffix = strings.TrimSpace(suffix)

	if tenantID == "" || deviceID == "" || suffix == "" {
		return "", ErrInvalidTopicPart
	}

	if strings.ContainsAny(tenantID, "/#+") || strings.ContainsAny(deviceID, "/#+") || strings.ContainsAny(suffix, "/#+") {
		return "", ErrInvalidTopicPart
	}

	return fmt.Sprintf("tenant/%s/device/%s/%s", tenantID, deviceID, suffix), nil
}
