package contracts

import "errors"

type CommandStatus string

const (
	CommandStatusCreated CommandStatus = "created"
	// CommandStatusPublished means the command was written to Kafka but the
	// MQTT downlink has not been confirmed by device-worker yet.
	CommandStatusPublished CommandStatus = "published"
	// CommandStatusSent means device-worker delivered the command over MQTT.
	CommandStatusSent    CommandStatus = "sent"
	CommandStatusAcked   CommandStatus = "acked"
	CommandStatusFailed  CommandStatus = "failed"
	CommandStatusTimeout CommandStatus = "timeout"
)

type CommandEvent string

const (
	CommandEventPublished CommandEvent = "published"
	// CommandEventDelivered marks the MQTT downlink succeeded in device-worker.
	CommandEventDelivered CommandEvent = "delivered"
	CommandEventAcked     CommandEvent = "acked"
	CommandEventFailed    CommandEvent = "failed"
	CommandEventTimeout   CommandEvent = "timeout"
)

var ErrInvalidCommandTransition = errors.New("invalid command transition")

func AdvanceCommandStatus(current CommandStatus, event CommandEvent) (CommandStatus, error) {
	switch current {
	case CommandStatusCreated:
		switch event {
		case CommandEventPublished:
			return CommandStatusPublished, nil
		case CommandEventAcked:
			// A device can ACK immediately after MQTT publish, before the
			// dispatcher commits the published transition.
			return CommandStatusAcked, nil
		}
	case CommandStatusPublished:
		switch event {
		case CommandEventDelivered:
			return CommandStatusSent, nil
		case CommandEventAcked:
			// Fast ACK may arrive before the delivery transition is committed.
			return CommandStatusAcked, nil
		case CommandEventFailed:
			return CommandStatusFailed, nil
		}
	case CommandStatusSent:
		switch event {
		case CommandEventAcked:
			return CommandStatusAcked, nil
		case CommandEventFailed:
			return CommandStatusFailed, nil
		case CommandEventTimeout:
			return CommandStatusTimeout, nil
		}
	}
	return "", ErrInvalidCommandTransition
}
