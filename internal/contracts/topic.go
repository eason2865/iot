package contracts

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidTopicPart = errors.New("invalid topic part")

const (
	TopicSuffixTelemetry = "telemetry"
	TopicSuffixCommand   = "command"
	TopicSuffixAck       = "ack"

	TelemetryTopicFilter = "tenant/+/device/+/telemetry"
	AckTopicFilter       = "tenant/+/device/+/ack"

	// SharedTelemetryTopicFilter is the default multi-replica-safe subscription
	// for telemetry-ingestor; the share group load-balances across replicas.
	SharedTelemetryTopicFilter = "$share/iot-telemetry/tenant/+/device/+/telemetry"
	// SharedAckTopicFilter is the default shared subscription for device-worker ACKs.
	SharedAckTopicFilter = "$share/iot-device-worker/tenant/+/device/+/ack"
)

func BuildDeviceTopic(tenantID, deviceID, suffix string) (string, error) {
	if !IsValidTenantID(tenantID) || !IsValidTopicPart(deviceID) || !IsValidTopicPart(suffix) {
		return "", ErrInvalidTopicPart
	}

	return fmt.Sprintf("tenant/%s/device/%s/%s", tenantID, deviceID, suffix), nil
}

func IsValidTopicPart(part string) bool {
	return part == strings.TrimSpace(part) && !strings.ContainsAny(part, "/#+") &&
		ValidateStorageField("topic part", part, TelemetryFieldMaxBytes) == nil
}

// The first colon separates tenantId from deviceId in MQTT usernames. Device
// IDs may contain further colons, but tenant IDs must never contain one.
func IsValidTenantID(id string) bool {
	return IsValidTopicPart(id) && !strings.Contains(id, ":")
}

func BuildDeviceUsername(tenantID, deviceID string) (string, error) {
	if !IsValidTenantID(tenantID) || !IsValidTopicPart(deviceID) {
		return "", ErrInvalidTopicPart
	}
	return tenantID + ":" + deviceID, nil
}

func ParseDeviceUsername(username string) (tenantID, deviceID string, ok bool) {
	tenantID, deviceID, ok = strings.Cut(username, ":")
	if !ok || !IsValidTenantID(tenantID) || !IsValidTopicPart(deviceID) {
		return "", "", false
	}
	return tenantID, deviceID, true
}

func BuildTelemetryTopic(tenantID, deviceID string) (string, error) {
	return BuildDeviceTopic(tenantID, deviceID, TopicSuffixTelemetry)
}

func BuildCommandTopic(tenantID, deviceID string) (string, error) {
	return BuildDeviceTopic(tenantID, deviceID, TopicSuffixCommand)
}

func BuildAckTopic(tenantID, deviceID string) (string, error) {
	return BuildDeviceTopic(tenantID, deviceID, TopicSuffixAck)
}

func BuildTenantCommandTopicFilter(tenantID string) (string, error) {
	if !IsValidTenantID(tenantID) {
		return "", ErrInvalidTopicPart
	}
	return fmt.Sprintf("tenant/%s/device/+/%s", tenantID, TopicSuffixCommand), nil
}

// ParseDeviceTopic extracts the tenant and device identifiers from a canonical
// device topic of the form tenant/{tenantId}/device/{deviceId}/{suffix}. It
// returns ok=false when the topic does not match the expected shape.
func ParseDeviceTopic(topic string) (tenantID, deviceID, suffix string, ok bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != "tenant" || parts[2] != "device" {
		return "", "", "", false
	}
	if !IsValidTenantID(parts[1]) || !IsValidTopicPart(parts[3]) || !IsValidTopicPart(parts[4]) {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[4], true
}
