package core

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"iot/internal/contracts"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

type Service struct {
	corev1.UnimplementedCoreServiceServer
	repo      platform.Repository
	publisher platform.MessagePublisher
}

func NewService(repo platform.Repository, publisher platform.MessagePublisher) *Service {
	return &Service{repo: repo, publisher: publisher}
}

func (s *Service) CreateTenant(_ context.Context, req *corev1.CreateTenantRequest) (*corev1.Tenant, error) {
	if req.GetId() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "id and name are required")
	}
	if !contracts.IsValidTopicPart(req.GetId()) {
		return nil, status.Error(codes.InvalidArgument, "tenantId contains invalid MQTT topic characters")
	}
	tenant, err := s.repo.CreateTenant(platform.Tenant{ID: req.GetId(), Name: req.GetName()})
	if err != nil {
		return nil, mapRepoError(err)
	}
	return &corev1.Tenant{Id: tenant.ID, Name: tenant.Name}, nil
}

func (s *Service) ListTenants(_ context.Context, req *corev1.ListTenantsRequest) (*corev1.ListTenantsResponse, error) {
	var tenants []platform.Tenant
	nextCursor := ""
	if paged, ok := s.repo.(interface {
		ListTenantsPage(platform.PageRequest) ([]platform.Tenant, string, error)
	}); ok {
		var err error
		tenants, nextCursor, err = paged.ListTenantsPage(platform.PageRequest{Size: int(req.GetPageSize()), Cursor: req.GetCursor()})
		if err != nil {
			return nil, mapRepoError(err)
		}
	} else {
		tenants = s.repo.ListTenants()
	}
	out := make([]*corev1.Tenant, 0, len(tenants))
	for _, tenant := range tenants {
		out = append(out, &corev1.Tenant{Id: tenant.ID, Name: tenant.Name})
	}
	return &corev1.ListTenantsResponse{Tenants: out, NextCursor: nextCursor}, nil
}

func (s *Service) CreateDevice(_ context.Context, req *corev1.CreateDeviceRequest) (*corev1.Device, error) {
	if req.GetTenantId() == "" || req.GetDeviceId() == "" || req.GetProductId() == "" || req.GetSecret() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenantId, deviceId, productId and secret are required")
	}
	if !contracts.IsValidTopicPart(req.GetTenantId()) || !contracts.IsValidTopicPart(req.GetDeviceId()) {
		return nil, status.Error(codes.InvalidArgument, "tenantId or deviceId contains invalid MQTT topic characters")
	}
	device, err := s.repo.CreateDevice(platform.Device{
		TenantID:  req.GetTenantId(),
		DeviceID:  req.GetDeviceId(),
		ProductID: req.GetProductId(),
		Secret:    req.GetSecret(),
	})
	if err != nil {
		return nil, mapRepoError(err)
	}
	return deviceToPB(device), nil
}

func (s *Service) ListDevices(_ context.Context, req *corev1.ListDevicesRequest) (*corev1.ListDevicesResponse, error) {
	var devices []platform.Device
	nextCursor := ""
	if paged, ok := s.repo.(interface {
		ListDevicesPage(platform.PageRequest) ([]platform.Device, string, error)
	}); ok {
		var err error
		devices, nextCursor, err = paged.ListDevicesPage(platform.PageRequest{Size: int(req.GetPageSize()), Cursor: req.GetCursor()})
		if err != nil {
			return nil, mapRepoError(err)
		}
	} else {
		devices = s.repo.ListDevices()
	}
	out := make([]*corev1.Device, 0, len(devices))
	for _, device := range devices {
		out = append(out, deviceToPB(device))
	}
	return &corev1.ListDevicesResponse{Devices: out, NextCursor: nextCursor}, nil
}

func (s *Service) GetDevice(_ context.Context, req *corev1.GetDeviceRequest) (*corev1.GetDeviceResponse, error) {
	device, ok := s.repo.GetDevice(req.GetTenantId(), req.GetDeviceId())
	if !ok {
		return nil, status.Error(codes.NotFound, "device not found")
	}
	return &corev1.GetDeviceResponse{Device: deviceToPB(device)}, nil
}

func (s *Service) GetDeviceStatus(_ context.Context, req *corev1.GetDeviceStatusRequest) (*corev1.GetDeviceStatusResponse, error) {
	deviceStatus, ok := s.repo.GetDeviceStatus(req.GetTenantId(), req.GetDeviceId())
	if !ok {
		return nil, status.Error(codes.NotFound, "device not found")
	}
	return &corev1.GetDeviceStatusResponse{Status: deviceStatusToPB(deviceStatus)}, nil
}

func (s *Service) ListTelemetry(_ context.Context, req *corev1.ListTelemetryRequest) (*corev1.ListTelemetryResponse, error) {
	var records []platform.TelemetryRecord
	nextCursor := ""
	if paged, ok := s.repo.(interface {
		ListTelemetryPage(platform.PageRequest) ([]platform.TelemetryRecord, string, error)
	}); ok {
		var err error
		records, nextCursor, err = paged.ListTelemetryPage(platform.PageRequest{Size: int(req.GetPageSize()), Cursor: req.GetCursor(), TenantID: req.GetTenantId(), DeviceID: req.GetDeviceId()})
		if err != nil {
			return nil, mapRepoError(err)
		}
	} else {
		records = s.repo.ListTelemetry(req.GetTenantId(), req.GetDeviceId())
	}
	out := make([]*corev1.TelemetryRecord, 0, len(records))
	for _, record := range records {
		out = append(out, telemetryToPB(record))
	}
	return &corev1.ListTelemetryResponse{Records: out, NextCursor: nextCursor}, nil
}

func (s *Service) IngestTelemetry(ctx context.Context, req *corev1.IngestTelemetryRequest) (*corev1.IngestTelemetryResponse, error) {
	envelope := envelopeFromPB(req)
	// The REST ingest path must enforce the same envelope contract as the MQTT
	// path; otherwise malformed records reach Kafka and poison the worker/DLQ.
	if err := contracts.ValidateEnvelope(envelope); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid telemetry envelope: %v", err)
	}
	record, err := s.repo.RecordTelemetry(envelope)
	if err != nil && !platform.IsTelemetryDuplicate(err) {
		return nil, mapRepoError(err)
	}
	// Always publish to Kafka so the telemetry reaches device-worker and
	// TDengine through the same unified event path as MQTT-originated data.
	// This must also run on the duplicate path: a previous attempt may have
	// written PostgreSQL but failed to publish to Kafka (e.g. broker outage),
	// in which case the row exists but the event never reached the worker.
	// Republishing is safe — the worker's duplicate handling makes the
	// PostgreSQL re-write idempotent and compensates the TDengine sink via
	// tdengine_written.
	if s.publisher != nil {
		if err := s.publisher.PublishTelemetry(ctx, record); err != nil {
			// The row is stored, so the caller may retry: a resend is idempotent
			// and covers the "PostgreSQL committed but Kafka did not" window.
			return nil, status.Errorf(codes.Unavailable, "telemetry stored but not published: %v", err)
		}
	}
	return &corev1.IngestTelemetryResponse{Record: telemetryToPB(record)}, nil
}

func (s *Service) RecordTelemetry(_ context.Context, req *corev1.RecordTelemetryRequest) (*corev1.RecordTelemetryResponse, error) {
	envelope := envelopeFromRecordPB(req.GetTelemetry())
	if err := contracts.ValidateEnvelope(envelope); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid telemetry envelope: %v", err)
	}
	record, err := s.repo.RecordTelemetry(envelope)
	if err != nil {
		if platform.IsTelemetryDuplicate(err) {
			return &corev1.RecordTelemetryResponse{Record: telemetryToPB(record)}, nil
		}
		return nil, mapRepoError(err)
	}
	return &corev1.RecordTelemetryResponse{Record: telemetryToPB(record)}, nil
}

func (s *Service) CreateCommand(ctx context.Context, req *corev1.CreateCommandRequest) (*corev1.CreateCommandResponse, error) {
	if req.GetTenantId() == "" || req.GetDeviceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenantId and deviceId are required")
	}
	if !contracts.IsValidTopicPart(req.GetTenantId()) || !contracts.IsValidTopicPart(req.GetDeviceId()) {
		return nil, status.Error(codes.InvalidArgument, "tenantId or deviceId contains invalid MQTT topic characters")
	}
	command, err := s.repo.CreateCommand(req.GetTenantId(), req.GetDeviceId(), req.GetPayload())
	if err != nil {
		return nil, mapRepoError(err)
	}
	// When the repository supports async dispatch (CommandDispatchStore), the
	// background CommandDispatcher claims and publishes 'created' commands.
	// Publishing synchronously here too would deliver the command twice.
	if _, dispatchesAsync := s.repo.(platform.CommandDispatchStore); s.publisher != nil && !dispatchesAsync {
		if err := s.publisher.PublishCommand(ctx, command); err != nil {
			return nil, status.Errorf(codes.Unavailable, "command created but not published: %v", err)
		}
	}
	return &corev1.CreateCommandResponse{Command: commandToPB(command)}, nil
}

func (s *Service) ListCommands(_ context.Context, req *corev1.ListCommandsRequest) (*corev1.ListCommandsResponse, error) {
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenantId is required")
	}
	if !contracts.IsValidTopicPart(req.GetTenantId()) {
		return nil, status.Error(codes.InvalidArgument, "tenantId contains invalid MQTT topic characters")
	}
	var commands []platform.Command
	nextCursor := ""
	if paged, ok := s.repo.(interface {
		ListCommandsPage(platform.PageRequest) ([]platform.Command, string, error)
	}); ok {
		var err error
		commands, nextCursor, err = paged.ListCommandsPage(platform.PageRequest{Size: int(req.GetPageSize()), Cursor: req.GetCursor(), TenantID: req.GetTenantId()})
		if err != nil {
			return nil, mapRepoError(err)
		}
	} else {
		for _, command := range s.repo.ListCommands() {
			if command.TenantID == req.GetTenantId() {
				commands = append(commands, command)
			}
		}
	}
	out := make([]*corev1.Command, 0, len(commands))
	for _, command := range commands {
		out = append(out, commandToPB(command))
	}
	return &corev1.ListCommandsResponse{Commands: out, NextCursor: nextCursor}, nil
}

func (s *Service) GetCommand(_ context.Context, req *corev1.GetCommandRequest) (*corev1.GetCommandResponse, error) {
	// Command reads are tenant-scoped, like ListCommands and AckCommand. This was
	// the only lookup that accepted a bare ID, which made the three command
	// endpoints disagree about their contract.
	if req.GetId() == "" || req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id and tenantId are required")
	}
	command, ok := s.repo.GetCommand(req.GetId())
	// A command that exists in another tenant is reported as not found, so the
	// response cannot confirm that another tenant's data exists.
	if !ok || command.TenantID != req.GetTenantId() {
		return nil, status.Error(codes.NotFound, "command not found")
	}
	return &corev1.GetCommandResponse{Command: commandToPB(command)}, nil
}

func (s *Service) AckCommand(_ context.Context, req *corev1.AckCommandRequest) (*corev1.AckCommandResponse, error) {
	if req.GetId() == "" || req.GetTenantId() == "" || req.GetDeviceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id, tenantId and deviceId are required")
	}
	command, err := s.repo.AckCommand(req.GetId(), req.GetTenantId(), req.GetDeviceId())
	if err != nil {
		return nil, mapRepoError(err)
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

// mapRepoError converts a repository error into a gRPC status error. The REST
// gateway maps status codes to HTTP statuses, so neither layer has to match on
// message text; doing that by substring previously turned "invalid topic
// characters" into 502 and "does not belong to device" into 502 as well.
func mapRepoError(err error) error {
	switch {
	case err == nil:
		return nil
	case platform.IsNotFound(err):
		return status.Error(codes.NotFound, err.Error())
	case platform.IsAlreadyExists(err):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
