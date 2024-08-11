package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	POLL_LIMIT = 200
)

type Message any

type Offset uint

type Log struct {
	messages       []Message
	startingOffset uint
	mutex          *sync.Mutex
}

func NewLog(startingOffset uint, message Message) *Log {
	return &Log{[]Message{message}, startingOffset, &sync.Mutex{}}
}

func (v *Log) Add(message Message) Offset {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.messages = append(v.messages, message)
	return Offset(v.startingOffset + uint(len(v.messages)-1))
}

func (v *Log) Read(limit uint, offset uint) []struct {
	Offset
	Message
} {
	startAt := offset - v.startingOffset
	var result []struct {
		Offset
		Message
	}
	for i := int(startAt); i < len(v.messages); i++ {
		result = append(result, struct {
			Offset
			Message
		}{Offset(uint(i) + startAt), v.messages[i]})
	}
	return result
}

type State struct {
	logs            map[string]*Log
	commitedOffsets map[string]Offset
	mutex           *sync.Mutex
}

func NewState() *State {
	return &State{make(map[string]*Log), make(map[string]Offset), &sync.Mutex{}}
}

func (v *State) Add(key string, message Message) Offset {
	v.mutex.Lock()
	log, exists := v.logs[key]
	if !exists {
		v.logs[key] = NewLog(0, message)
		v.mutex.Unlock()
		return 1
	}
	v.mutex.Unlock()
	return log.Add(message)
}

func (v *State) Commit(offsets map[string]Offset) {
	// Assuming a single consumer for particular key
	for key, toCommit := range offsets {
		v.commitedOffsets[key] = toCommit
	}
}

func (v *State) ReadMessages(limit uint, offsets map[string]Offset) map[string][]struct {
	Offset
	Message
} {
	results := map[string][]struct {
		Offset
		Message
	}{}
	for key, offset := range offsets {
		if log, exists := v.logs[key]; exists {
			messages := log.Read(limit, uint(offset))
			if len(messages) > 0 {
				results[key] = messages
			}
		}
	}
	return results
}

func (v *State) ListOffsets(keys []string) map[string]Offset {
	result := map[string]Offset{}
	for _, key := range keys {
		offset, exists := v.commitedOffsets[key]
		if exists {
			result[key] = offset
		}
	}
	return result
}

type SendBody struct {
	Key     string  `json:"key"`
	Message Message `json:"msg"`
}

type PollBody struct {
	Offsets map[string]Offset `json:"offsets"`
}

type CommitOffsetsBody struct {
	Offsets map[string]Offset `json:"offsets"`
}

type ListCommitedOffsetsBody struct {
	Keys []string `json:"keys"`
}

func main() {
	node := maelstrom.NewNode()
	state := NewState()

	node.Handle("send", func(msg maelstrom.Message) error {
		var body SendBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offset := state.Add(body.Key, body.Message)
		response := map[string]any{"type": "send_ok", "offset": offset}

		return node.Reply(msg, response)
	})
	node.Handle("poll", func(msg maelstrom.Message) error {
		var body PollBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		messages := state.ReadMessages(POLL_LIMIT, body.Offsets)
		polled := map[string][][]any{}
		for key, list := range messages {
			var mapped [][]any
			for i := range list {
				mapped = append(mapped, []any{list[i].Offset, list[i].Message})
			}
			polled[key] = mapped
		}
		response := map[string]any{"type": "poll_ok", "msgs": polled}

		return node.Reply(msg, response)
	})
	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body CommitOffsetsBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.Commit(body.Offsets)
		response := map[string]any{"type": "commit_offsets_ok"}

		return node.Reply(msg, response)
	})
	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body ListCommitedOffsetsBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := state.ListOffsets(body.Keys)
		response := map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets}

		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
