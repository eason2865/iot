package core

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "iot/proto/core/v1"
)

func TestRPCValidatesIdentityAndListFilters(t *testing.T) {
	svc := NewService(fakeRepo{}, nil)
	for name, call := range map[string]func() error{
		"device": func() error { _, err := svc.GetDevice(t.Context(), &corev1.GetDeviceRequest{}); return err },
		"status": func() error {
			_, err := svc.GetDeviceStatus(t.Context(), &corev1.GetDeviceStatusRequest{TenantId: "t", DeviceId: "bad/#"})
			return err
		},
		"device-list": func() error { _, err := svc.ListDevices(t.Context(), &corev1.ListDevicesRequest{}); return err },
		"telemetry-list": func() error {
			_, err := svc.ListTelemetry(t.Context(), &corev1.ListTelemetryRequest{TenantId: "t"})
			return err
		},
		"sort": func() error {
			_, err := svc.ListCommands(t.Context(), &corev1.ListCommandsRequest{TenantId: "t", Sort: "invalid"})
			return err
		},
		"time": func() error {
			_, err := svc.ListCommands(t.Context(), &corev1.ListCommandsRequest{TenantId: "t", StartTime: &timestamppb.Timestamp{Seconds: 253402300800}})
			return err
		},
		"precision": func() error {
			_, err := svc.ListCommands(t.Context(), &corev1.ListCommandsRequest{TenantId: "t", StartTime: &timestamppb.Timestamp{Seconds: 1, Nanos: 1}})
			return err
		},
		"result": func() error {
			_, err := svc.AckCommand(t.Context(), &corev1.AckCommandRequest{Id: "c", TenantId: "t", DeviceId: "d", Result: &corev1.CommandResult{Status: "unknown"}})
			return err
		},
	} {
		if err := call(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s code=%s err=%v", name, status.Code(err), err)
		}
	}
}
