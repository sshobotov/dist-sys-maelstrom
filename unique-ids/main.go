package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	var counter atomic.Uint32

	node.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"
		body["id"] = fmt.Sprintf("%v:%d", msg.Dest[1:], counter.Add(1))

		// Echo the original message back with the updated message type.
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
