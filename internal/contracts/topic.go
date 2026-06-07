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
)

func BuildDeviceTopic(tenantID, deviceID, suffix string) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	deviceID = strings.TrimSpace(deviceID)
	suffix = strings.TrimSpace(suffix)

	if !IsValidTopicPart(tenantID) || !IsValidTopicPart(deviceID) || !IsValidTopicPart(suffix) {
		return "", ErrInvalidTopicPart
	}

	return fmt.Sprintf("tenant/%s/device/%s/%s", tenantID, deviceID, suffix), nil
}

func IsValidTopicPart(part string) bool {
	part = strings.TrimSpace(part)
	return part != "" && !strings.ContainsAny(part, "/#+")
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
	tenantID = strings.TrimSpace(tenantID)
	if !IsValidTopicPart(tenantID) {
		return "", ErrInvalidTopicPart
	}
	return fmt.Sprintf("tenant/%s/device/+/%s", tenantID, TopicSuffixCommand), nil
}
