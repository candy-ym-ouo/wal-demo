package store

import (
	"encoding/json"
	"fmt"
)

const (
	opSet    = "set"
	opDelete = "delete"
)

type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func encodeCommand(op, key string, value []byte) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("key must not be empty")
	}
	return json.Marshal(command{Op: op, Key: key, Value: value})
}

func decodeCommand(payload []byte) (command, error) {
	var cmd command
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return command{}, fmt.Errorf("decode store command: %w", err)
	}
	if cmd.Key == "" {
		return command{}, fmt.Errorf("command key must not be empty")
	}
	if cmd.Op != opSet && cmd.Op != opDelete {
		return command{}, fmt.Errorf("unknown store operation %q", cmd.Op)
	}
	return cmd, nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
