package contracts

import "errors"

type CommandStatus string

const (
	CommandStatusCreated CommandStatus = "created"
	CommandStatusSent    CommandStatus = "sent"
	CommandStatusAcked   CommandStatus = "acked"
	CommandStatusFailed  CommandStatus = "failed"
	CommandStatusTimeout CommandStatus = "timeout"
)

type CommandEvent string

const (
	CommandEventPublished CommandEvent = "published"
	CommandEventAcked     CommandEvent = "acked"
	CommandEventFailed    CommandEvent = "failed"
	CommandEventTimeout   CommandEvent = "timeout"
)

var ErrInvalidCommandTransition = errors.New("invalid command transition")

func AdvanceCommandStatus(current CommandStatus, event CommandEvent) (CommandStatus, error) {
	switch current {
	case CommandStatusCreated:
		if event == CommandEventPublished {
			return CommandStatusSent, nil
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
