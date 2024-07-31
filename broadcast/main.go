package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TopologyBody struct {
	Topology map[string][]string `json:"topology"`
}
type BroadcastBody struct {
	Message int64 `json:"message"`
	MsgId   int64 `json:"msg_id"`
}
type GossipBody struct {
	Messages []MessageInput `json:"messages"`
}
type MessageInput struct {
	Message  int64  `json:"message"`
	OriginId string `json:"origin_id"`
}

func main() {
	node := maelstrom.NewNode()
	inputs := make(chan []MessageInput)
	syncStates := make(chan struct {
		string
		int
	})
	var topology []string
	var seenValues []int64
	inputsApplied := map[string]struct{}{}
	originsOfValues := map[int]string{}
	neighborsSyncState := map[string]int{}

	sync := func() {
		for i := range topology {
			neighbor := topology[i]
			offset, exists := neighborsSyncState[neighbor]
			go func() {
				firstIndex := offset
				if exists {
					firstIndex = offset + 1
				}
				lastIndex := len(seenValues) - 1
				var syncValues []MessageInput
				for i := firstIndex; i <= lastIndex; i++ {
					valueToSync := MessageInput{seenValues[i], originsOfValues[i]}
					syncValues = append(syncValues, valueToSync)
				}
				if len(syncValues) < 1 {
					return
				}

				payload := map[string]any{"type": "gossip", "messages": syncValues}
				fmt.Fprintf(os.Stderr, "Syncing from %s to %s: %v\n", node.ID(), neighbor, payload)

				err := node.RPC(neighbor, payload, func(msg maelstrom.Message) error {
					fmt.Fprintf(os.Stderr, "Synced from %s to %s\n", node.ID(), neighbor)
					syncStates <- struct {
						string
						int
					}{neighbor, lastIndex}
					return nil
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to sync from %s to %s: %v\n", node.ID(), neighbor, err)
				}
			}()
		}
	}

	go func() {
		for entries := range inputs {
			var hasUpdate bool
			for i := range entries {
				entry := entries[i]
				if _, exists := inputsApplied[entry.OriginId]; !exists {
					seenValues = append(seenValues, entry.Message)
					inputsApplied[entry.OriginId] = struct{}{}
					originsOfValues[len(seenValues)-1] = entry.OriginId

					hasUpdate = true
				}
			}
			if hasUpdate {
				sync()
			}
		}
	}()
	go func() {
		for value := range syncStates {
			if offset := neighborsSyncState[value.string]; offset < value.int {
				neighborsSyncState[value.string] = value.int
			}
		}
	}()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		originId := fmt.Sprintf("%s:%d", msg.Dest, body.MsgId)

		inputs <- []MessageInput{{body.Message, originId}}

		response := map[string]any{"type": "broadcast_ok"}
		return node.Reply(msg, response)
	})
	node.Handle("gossip", func(msg maelstrom.Message) error {
		var body GossipBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		inputs <- body.Messages

		response := map[string]any{"type": "gossip_ok"}
		return node.Reply(msg, response)
	})
	node.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{"type": "read_ok", "messages": seenValues}

		return node.Reply(msg, response)
	})
	node.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topology = body.Topology[msg.Dest]
		log.Printf("New known topology on %s: %v\n", msg.Dest, topology)

		response := map[string]any{"type": "topology_ok"}
		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
