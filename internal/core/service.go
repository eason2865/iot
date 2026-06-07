package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"iot/internal/contracts"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

type Repository interface {
	CreateTenant(platform.Tenant) (platform.Tenant, error)
	ListTenants() []platform.Tenant
	CreateDevice(platform.Device) (platform.Device, error)
	ListDevices() []platform.Device
	GetDevice(tenantID, deviceID string) (platform.Device, bool)
	RecordTelemetry(env contracts.Envelope) (platform.TelemetryRecord, error)
	ListTelemetry(tenantID, deviceID string) []platform.TelemetryRecord
	GetDeviceStatus(tenantID, deviceID string) (platform.DeviceStatus, bool)
	CreateCommand(tenantID, deviceID string, payload json.RawMessage) (platform.Command, error)
	AckCommand(id, tenantID, deviceID string) (platform.Command, error)
	ListCommands() []platform.Command
	GetCommand(id string) (platform.Command, bool)
}

type Publisher interface {
	PublishTelemetry(platform.TelemetryRecord) error
	PublishCommand(platform.Command) error
}

type Service struct {
	corev1.UnimplementedCoreServiceServer
	repo      Repository
	publisher Publisher
}

func NewService(repo Repository, publisher Publisher) *Service {
	return &Service{repo: repo, publisher: publisher}
}

func (s *Service) CreateTenant(_ context.Context, req *corev1.CreateTenantRequest) (*corev1.Tenant, error) {
	if req.GetId() == "" || req.GetName() == "" {
		return nil, fmt.Errorf("id and name are required")
	}
	if !contracts.IsValidTopicPart(req.GetId()) {
		return nil, fmt.Errorf("tenantId contains invalid MQTT topic characters")
	}
	tenant, err := s.repo.CreateTenant(platform.Tenant{ID: req.GetId(), Name: req.GetName()})
	if err != nil {
		return nil, err
	}
	return &corev1.Tenant{Id: tenant.ID, Name: tenant.Name}, nil
}

func (s *Service) ListTenants(context.Context, *corev1.ListTenantsRequest) (*corev1.ListTenantsResponse, error) {
	tenants := s.repo.ListTenants()
	out := make([]*corev1.Tenant, 0, len(tenants))
	for _, tenant := range tenants {
		out = append(out, &corev1.Tenant{Id: tenant.ID, Name: tenant.Name})
	}
	return &corev1.ListTenantsResponse{Tenants: out}, nil
}

func (s *Service) CreateDevice(_ context.Context, req *corev1.CreateDeviceRequest) (*corev1.Device, error) {
	if req.GetTenantId() == "" || req.GetDeviceId() == "" || req.GetProductId() == "" || req.GetSecret() == "" {
		return nil, fmt.Errorf("tenantId, deviceId, productId and secret are required")
	}
	if !contracts.IsValidTopicPart(req.GetTenantId()) || !contracts.IsValidTopicPart(req.GetDeviceId()) {
		return nil, fmt.Errorf("tenantId or deviceId contains invalid MQTT topic characters")
	}
	device, err := s.repo.CreateDevice(platform.Device{
		TenantID:  req.GetTenantId(),
		DeviceID:  req.GetDeviceId(),
		ProductID: req.GetProductId(),
		Secret:    req.GetSecret(),
	})
	if err != nil {
		return nil, err
	}
	return deviceToPB(device), nil
}

func (s *Service) ListDevices(context.Context, *corev1.ListDevicesRequest) (*corev1.ListDevicesResponse, error) {
	devices := s.repo.ListDevices()
	out := make([]*corev1.Device, 0, len(devices))
	for _, device := range devices {
		out = append(out, deviceToPB(device))
	}
	return &corev1.ListDevicesResponse{Devices: out}, nil
}

func (s *Service) GetDevice(_ context.Context, req *corev1.GetDeviceRequest) (*corev1.GetDeviceResponse, error) {
	device, ok := s.repo.GetDevice(req.GetTenantId(), req.GetDeviceId())
	if !ok {
		return nil, fmt.Errorf("device not found")
	}
	return &corev1.GetDeviceResponse{Device: deviceToPB(device)}, nil
}

func (s *Service) GetDeviceStatus(_ context.Context, req *corev1.GetDeviceStatusRequest) (*corev1.GetDeviceStatusResponse, error) {
	status, ok := s.repo.GetDeviceStatus(req.GetTenantId(), req.GetDeviceId())
	if !ok {
		return nil, fmt.Errorf("device not found")
	}
	return &corev1.GetDeviceStatusResponse{Status: deviceStatusToPB(status)}, nil
}

func (s *Service) ListTelemetry(_ context.Context, req *corev1.ListTelemetryRequest) (*corev1.ListTelemetryResponse, error) {
	records := s.repo.ListTelemetry(req.GetTenantId(), req.GetDeviceId())
	out := make([]*corev1.TelemetryRecord, 0, len(records))
	for _, record := range records {
		out = append(out, telemetryToPB(record))
	}
	return &corev1.ListTelemetryResponse{Records: out}, nil
}

func (s *Service) IngestTelemetry(_ context.Context, req *corev1.IngestTelemetryRequest) (*corev1.IngestTelemetryResponse, error) {
	record, err := s.repo.RecordTelemetry(envelopeFromPB(req))
	if err != nil {
		return nil, err
	}
	if s.publisher != nil {
		if err := s.publisher.PublishTelemetry(record); err != nil {
			return nil, err
		}
	}
	return &corev1.IngestTelemetryResponse{Record: telemetryToPB(record)}, nil
}

func (s *Service) RecordTelemetry(_ context.Context, req *corev1.RecordTelemetryRequest) (*corev1.RecordTelemetryResponse, error) {
	record, err := s.repo.RecordTelemetry(envelopeFromRecordPB(req.GetTelemetry()))
	if err != nil {
		return nil, err
	}
	return &corev1.RecordTelemetryResponse{Record: telemetryToPB(record)}, nil
}

func (s *Service) CreateCommand(_ context.Context, req *corev1.CreateCommandRequest) (*corev1.CreateCommandResponse, error) {
	if req.GetTenantId() == "" || req.GetDeviceId() == "" {
		return nil, fmt.Errorf("tenantId and deviceId are required")
	}
	if !contracts.IsValidTopicPart(req.GetTenantId()) || !contracts.IsValidTopicPart(req.GetDeviceId()) {
		return nil, fmt.Errorf("tenantId or deviceId contains invalid MQTT topic characters")
	}
	command, err := s.repo.CreateCommand(req.GetTenantId(), req.GetDeviceId(), req.GetPayload())
	if err != nil {
		return nil, err
	}
	if s.publisher != nil {
		if err := s.publisher.PublishCommand(command); err != nil {
			return nil, err
		}
	}
	return &corev1.CreateCommandResponse{Command: commandToPB(command)}, nil
}

func (s *Service) ListCommands(context.Context, *corev1.ListCommandsRequest) (*corev1.ListCommandsResponse, error) {
	commands := s.repo.ListCommands()
	out := make([]*corev1.Command, 0, len(commands))
	for _, command := range commands {
		out = append(out, commandToPB(command))
	}
	return &corev1.ListCommandsResponse{Commands: out}, nil
}

func (s *Service) GetCommand(_ context.Context, req *corev1.GetCommandRequest) (*corev1.GetCommandResponse, error) {
	command, ok := s.repo.GetCommand(req.GetId())
	if !ok {
		return nil, fmt.Errorf("command not found")
	}
	return &corev1.GetCommandResponse{Command: commandToPB(command)}, nil
}

func (s *Service) AckCommand(_ context.Context, req *corev1.AckCommandRequest) (*corev1.AckCommandResponse, error) {
	if req.GetId() == "" || req.GetTenantId() == "" || req.GetDeviceId() == "" {
		return nil, fmt.Errorf("id, tenantId and deviceId are required")
	}
	command, err := s.repo.AckCommand(req.GetId(), req.GetTenantId(), req.GetDeviceId())
	if err != nil {
		return nil, err
	}
	return &corev1.AckCommandResponse{Command: commandToPB(command)}, nil
}

func envelopeFromPB(req *corev1.IngestTelemetryRequest) contracts.Envelope {
	return contracts.Envelope{
		MsgID:    req.GetMsgId(),
		TenantID: req.GetTenantId(),
		DeviceID: req.GetDeviceId(),
		Ts:       req.GetTs(),
		Type:     req.GetType(),
		Version:  req.GetVersion(),
		Payload:  json.RawMessage(req.GetPayload()),
	}
}

func envelopeFromRecordPB(rec *corev1.TelemetryRecord) contracts.Envelope {
	return contracts.Envelope{
		MsgID:    rec.GetMsgId(),
		TenantID: rec.GetTenantId(),
		DeviceID: rec.GetDeviceId(),
		Ts:       rec.GetTs(),
		Type:     rec.GetType(),
		Version:  rec.GetVersion(),
		Payload:  json.RawMessage(rec.GetPayload()),
	}
}

func deviceToPB(device platform.Device) *corev1.Device {
	return &corev1.Device{
		TenantId:  device.TenantID,
		DeviceId:  device.DeviceID,
		ProductId: device.ProductID,
		Secret:    device.Secret,
		CreatedAt: toTimestamp(device.CreatedAt),
	}
}

func deviceStatusToPB(status platform.DeviceStatus) *corev1.DeviceStatus {
	return &corev1.DeviceStatus{
		TenantId:   status.TenantID,
		DeviceId:   status.DeviceID,
		Online:     status.Online,
		LastSeenAt: toTimestamp(status.LastSeenAt),
	}
}

func telemetryToPB(record platform.TelemetryRecord) *corev1.TelemetryRecord {
	return &corev1.TelemetryRecord{
		MsgId:      record.MsgID,
		TenantId:   record.TenantID,
		DeviceId:   record.DeviceID,
		Ts:         record.Ts,
		Type:       record.Type,
		Version:    record.Version,
		Payload:    []byte(record.Payload),
		ReceivedAt: toTimestamp(record.ReceivedAt),
	}
}

func commandToPB(command platform.Command) *corev1.Command {
	return &corev1.Command{
		Id:        command.ID,
		TenantId:  command.TenantID,
		DeviceId:  command.DeviceID,
		Status:    string(command.Status),
		Payload:   []byte(command.Payload),
		CreatedAt: toTimestamp(command.CreatedAt),
		UpdatedAt: toTimestamp(command.UpdatedAt),
	}
}

func toTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}
