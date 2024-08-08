package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddBody struct {
	Delta uint `json:"delta"`
}

const RESULT_KEY = "counter"

func readPersistedCounter(kv *maelstrom.KV) (int, bool) {
	value, err := kv.Read(context.Background(), RESULT_KEY)
	if err != nil {
		if rpcError, ok := err.(*maelstrom.RPCError); !ok || rpcError.Code != maelstrom.KeyDoesNotExist {
			log.Fatalf("Failed to read from KV: %v", err)
		} else {
			return 0, false
		}
	}
	return value.(int), true
}

func incrementPersistedCounter(kv *maelstrom.KV, value uint) {
	for {
		counter, exists := readPersistedCounter(kv)
		log.Printf("Received data from KV before CAS write: %v, %v", counter, exists)
		err := kv.CompareAndSwap(context.Background(), RESULT_KEY, counter, uint(counter)+value, !exists)
		if err != nil {
			log.Printf("Failed to CAS write to KV: %v", err)
		} else {
			break
		}
	}
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	node.Handle("add", func(msg maelstrom.Message) error {
		var body AddBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if body.Delta > 0 {
			incrementPersistedCounter(kv, body.Delta)
		}
		response := map[string]any{"type": "add_ok"}

		return node.Reply(msg, response)
	})
	node.Handle("read", func(msg maelstrom.Message) error {
		counter, _ := readPersistedCounter(kv)
		response := map[string]any{"type": "read_ok", "value": counter}

		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
