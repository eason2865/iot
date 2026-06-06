package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"iot/internal/contracts"
	"iot/internal/platform"
)

type AdminAPI interface {
	CreateTenant(ctx context.Context, id, name string) error
	CreateDevice(ctx context.Context, tenantID, deviceID, productID string) error
	CreateCommand(ctx context.Context, tenantID, deviceID string, payload json.RawMessage) (platform.Command, error)
}

type Bus interface {
	Publish(topic string, payload []byte) error
	Subscribe(topic string, handler func(topic string, payload []byte)) error
	Close() error
}

type BusFactory interface {
	NewClient(ctx context.Context, tenantID string) (Bus, error)
}

type Config struct {
	TenantCount       int
	DevicesPerTenant  int
	TenantPrefix      string
	ProductID         string
	TickInterval      time.Duration
	TelemetryBurstMin int
	TelemetryBurstMax int
	CommandBurstMin   int
	CommandBurstMax   int
	TelemetryWeight   int
	CommandWeight     int
	Metrics           *platform.Metrics
}

type Service struct {
	cfg     Config
	admin   AdminAPI
	factory BusFactory
	rng     *rand.Rand
	metrics *platform.Metrics

	mu      sync.RWMutex
	tenants []demoTenant
	agents  map[string]*tenantAgent
}

type demoTenant struct {
	ID      string
	Name    string
	Devices []demoDevice
}

type demoDevice struct {
	TenantID  string
	DeviceID  string
	ProductID string
}

type tenantAgent struct {
	tenantID string
	bus      Bus
	devices  []string
}

func NewService(cfg Config, admin AdminAPI, factory BusFactory, rng *rand.Rand) *Service {
	cfg = normalizeConfig(cfg)
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Service{
		cfg:     cfg,
		admin:   admin,
		factory: factory,
		rng:     rng,
		metrics: cfg.Metrics,
		agents:  map[string]*tenantAgent{},
	}
}

func (s *Service) EnsureTopology(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.tenants) > 0 {
		return nil
	}

	for ti := 0; ti < s.cfg.TenantCount; ti++ {
		tenantID := fmt.Sprintf("%s-tenant-%d", s.cfg.TenantPrefix, ti)
		tenant := demoTenant{
			ID:   tenantID,
			Name: fmt.Sprintf("Demo Tenant %d", ti+1),
		}
		tenantCtx := platform.ContextWithRequestID(ctx, platform.NewRequestID())
		if s.admin != nil {
			if err := s.admin.CreateTenant(tenantCtx, tenant.ID, tenant.Name); err != nil {
				if s.metrics != nil {
					s.metrics.IncDemo("topology", "error")
				}
				return err
			}
		}
		if s.metrics != nil {
			s.metrics.IncDemo("topology", "ok")
		}

		for di := 0; di < s.cfg.DevicesPerTenant; di++ {
			deviceID := fmt.Sprintf("%s-device-%d-%d", s.cfg.TenantPrefix, ti, di)
			dev := demoDevice{
				TenantID:  tenantID,
				DeviceID:  deviceID,
				ProductID: s.cfg.ProductID,
			}
			tenant.Devices = append(tenant.Devices, dev)
			if s.admin != nil {
				deviceCtx := platform.ContextWithRequestID(ctx, platform.NewRequestID())
				if err := s.admin.CreateDevice(deviceCtx, dev.TenantID, dev.DeviceID, dev.ProductID); err != nil {
					if s.metrics != nil {
						s.metrics.IncDemo("topology", "error")
					}
					return err
				}
			}
			if s.metrics != nil {
				s.metrics.IncDemo("topology", "ok")
			}
		}

		s.tenants = append(s.tenants, tenant)
	}

	for _, tenant := range s.tenants {
		if s.factory == nil {
			continue
		}
		bus, err := s.factory.NewClient(ctx, tenant.ID)
		if err != nil {
			return err
		}
		agent := &tenantAgent{
			tenantID: tenant.ID,
			bus:      bus,
			devices:  make([]string, 0, len(tenant.Devices)),
		}
		for _, dev := range tenant.Devices {
			agent.devices = append(agent.devices, dev.DeviceID)
		}
		if err := agent.subscribe(); err != nil {
			_ = bus.Close()
			return err
		}
		s.agents[tenant.ID] = agent
	}

	return nil
}

func (s *Service) EmitTelemetry(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.tenants) == 0 {
		return nil
	}
	tenant := s.tenants[s.rng.Intn(len(s.tenants))]
	if len(tenant.Devices) == 0 {
		return nil
	}
	device := tenant.Devices[s.rng.Intn(len(tenant.Devices))]

	topic, err := contracts.BuildDeviceTopic(tenant.ID, device.DeviceID, "telemetry")
	if err != nil {
		if s.metrics != nil {
			s.metrics.IncDemo("telemetry", "error")
		}
		return err
	}
	payload := map[string]any{
		"msgId":    fmt.Sprintf("demo-%d", time.Now().UnixNano()),
		"tenantId": tenant.ID,
		"deviceId": device.DeviceID,
		"ts":       time.Now().UnixMilli(),
		"type":     "telemetry",
		"version":  "v1",
		"payload": map[string]any{
			"temp":  20 + s.rng.Intn(10),
			"noise": s.rng.Intn(1000),
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	agent := s.agents[tenant.ID]
	if agent == nil || agent.bus == nil {
		return nil
	}
	if err := agent.bus.Publish(topic, body); err != nil {
		if s.metrics != nil {
			s.metrics.IncDemo("telemetry", "error")
		}
		return err
	}
	if s.metrics != nil {
		s.metrics.IncDemo("telemetry", "ok")
	}
	return nil
}

func (s *Service) EmitCommand(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.tenants) == 0 {
		return nil
	}
	tenant := s.tenants[s.rng.Intn(len(s.tenants))]
	if len(tenant.Devices) == 0 {
		return nil
	}
	device := tenant.Devices[s.rng.Intn(len(tenant.Devices))]
	if s.admin == nil {
		return nil
	}
	payload := json.RawMessage(fmt.Sprintf(`{"switch":"toggle","nonce":%d}`, s.rng.Int63()))
	commandCtx := platform.ContextWithRequestID(ctx, platform.NewRequestID())
	_, err := s.admin.CreateCommand(commandCtx, tenant.ID, device.DeviceID, payload)
	if err != nil {
		if s.metrics != nil {
			s.metrics.IncDemo("command", "error")
		}
		return err
	}
	if s.metrics != nil {
		s.metrics.IncDemo("command", "ok")
	}
	return nil
}

func (s *Service) Run(ctx context.Context) error {
	if err := s.waitForTopology(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	for {
		_ = s.runBurst(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) waitForTopology(ctx context.Context) error {
	backoff := 500 * time.Millisecond
	for {
		if err := s.EnsureTopology(ctx); err != nil {
			log.Printf("demo topology not ready: %v", err)
		} else {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (s *Service) runBurst(ctx context.Context) error {
	telemetryBurst := burstInRange(s.rng, s.cfg.TelemetryBurstMin, s.cfg.TelemetryBurstMax)
	commandBurst := burstInRange(s.rng, s.cfg.CommandBurstMin, s.cfg.CommandBurstMax)

	for i := 0; i < telemetryBurst; i++ {
		if err := s.EmitTelemetry(ctx); err != nil {
			log.Printf("demo telemetry error: %v", err)
		}
	}
	for i := 0; i < commandBurst; i++ {
		if err := s.EmitCommand(ctx); err != nil {
			log.Printf("demo command error: %v", err)
		}
	}
	return nil
}

func burstInRange(rng *rand.Rand, min, max int) int {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	if max == min {
		return min
	}
	return min + rng.Intn(max-min+1)
}

func normalizeConfig(cfg Config) Config {
	if cfg.TenantCount <= 0 {
		cfg.TenantCount = 3
	}
	if cfg.DevicesPerTenant <= 0 {
		cfg.DevicesPerTenant = 10
	}
	if strings.TrimSpace(cfg.TenantPrefix) == "" {
		cfg.TenantPrefix = "demo"
	}
	if strings.TrimSpace(cfg.ProductID) == "" {
		cfg.ProductID = "product-demo"
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 250 * time.Millisecond
	}
	if cfg.TelemetryBurstMin <= 0 {
		cfg.TelemetryBurstMin = 1
	}
	if cfg.TelemetryBurstMax < cfg.TelemetryBurstMin {
		cfg.TelemetryBurstMax = cfg.TelemetryBurstMin
	}
	if cfg.CommandBurstMin <= 0 {
		cfg.CommandBurstMin = 1
	}
	if cfg.CommandBurstMax < cfg.CommandBurstMin {
		cfg.CommandBurstMax = cfg.CommandBurstMin
	}
	if cfg.TelemetryWeight <= 0 && cfg.CommandWeight <= 0 {
		cfg.TelemetryWeight = 7
		cfg.CommandWeight = 3
	}
	if cfg.TelemetryWeight < 0 {
		cfg.TelemetryWeight = 0
	}
	if cfg.CommandWeight < 0 {
		cfg.CommandWeight = 0
	}
	return cfg
}

func (a *tenantAgent) subscribe() error {
	if a == nil || a.bus == nil {
		return nil
	}
	topic := fmt.Sprintf("tenant/%s/device/+/command", a.tenantID)
	return a.bus.Subscribe(topic, func(topic string, payload []byte) {
		var downlink platform.CommandDownlink
		if err := json.Unmarshal(payload, &downlink); err != nil {
			return
		}
		ackTopic, err := contracts.BuildDeviceTopic(downlink.TenantID, downlink.DeviceID, "ack")
		if err != nil {
			return
		}
		ack := platform.CommandAckMessage{
			CommandID: downlink.ID,
			TenantID:  downlink.TenantID,
			DeviceID:  downlink.DeviceID,
			Status:    "acked",
		}
		body, err := json.Marshal(ack)
		if err != nil {
			return
		}
		_ = a.bus.Publish(ackTopic, body)
	})
}

func topicMatches(pattern, topic string) bool {
	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")
	if len(pParts) != len(tParts) {
		return false
	}
	for i := range pParts {
		switch pParts[i] {
		case "+":
			continue
		default:
			if pParts[i] != tParts[i] {
				return false
			}
		}
	}
	return true
}
