package platform

import "sync"

// Each connection gets a new generation. A delayed SUBACK from an old
// connection must not make a reconnect or a stopping process ready.
type mqttReadiness struct {
	mu         sync.RWMutex
	generation uint64
	subscribed bool
	stopped    bool
}

func (s *mqttReadiness) reset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
	s.subscribed = false
	return s.generation
}

func (s *mqttReadiness) confirm(generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation == s.generation && !s.stopped {
		s.subscribed = true
		return true
	}
	return false
}

func (s *mqttReadiness) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
	s.subscribed = false
	s.stopped = true
}

func (s *mqttReadiness) ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subscribed && !s.stopped
}
