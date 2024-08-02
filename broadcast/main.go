package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Message int64
type NodeID string

type TopologyBody struct {
	Topology map[NodeID][]NodeID `json:"topology"`
}
type BroadcastBody struct {
	Message Message `json:"message"`
	MsgId   int64   `json:"msg_id"`
}
type RetryInput struct {
	Messages []Message
	NodeID   NodeID
}
type ForwardBody struct {
	Messages []Message `json:"messages"`
}

// 25 nodes grid -> 2 * sqrt(n) = 10 network delays
// 100ms latency -> ~3 hops at max
// <30 per broadcast -> ~1 message per node per broadcast request
// rate=100 -> ~4 broadcasts per second per node
func main() {
	node := maelstrom.NewNode()
	inputs := make(chan Message)
	retries := make(chan RetryInput)
	attemptedRetries := make(chan RetryInput)
	messages := []Message{}
	toRetry := map[NodeID][]Message{}

	debugLogger := log.New(os.Stderr, "DEBUG ", log.LstdFlags)

	go func() {
		for input := range inputs {
			messages = append(messages, input)
		}
	}()
	go func() {
		for {
			select {
			case retry := <-retries:
				toRetry[retry.NodeID] = append(toRetry[retry.NodeID], retry.Messages...)
				debugLogger.Printf("Added messages to retry for %s: %v\n", retry.NodeID, retry.Messages)
			case retry := <-attemptedRetries:
				cleaned := []Message{}
				removable := map[Message]struct{}{}
				for _, message := range retry.Messages {
					removable[message] = struct{}{}
				}
				for _, message := range toRetry[retry.NodeID] {
					if _, exists := removable[message]; !exists {
						cleaned = append(cleaned, message)
					}
				}
				toRetry[retry.NodeID] = cleaned
				debugLogger.Printf("Removed messages to retry for %s: %v\n", retry.NodeID, retry.Messages)
			}
		}
	}()

	node.Handle("topology", func(msg maelstrom.Message) error {
		// Ignore topology suggestion
		response := map[string]any{"type": "topology_ok"}
		return node.Reply(msg, response)
	})
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		inputs <- body.Message

		// go func() {
		// Send to other nodes
		wg := sync.WaitGroup{}
		ctx, cancelCtx := context.WithTimeout(context.Background(), 250*time.Millisecond)
		for _, nodeID := range node.NodeIDs() {
			if nodeID != node.ID() {
				wg.Add(1)
				go func() {
					toRetry := toRetry[NodeID(nodeID)][:]
					if len(toRetry) > 0 {
						attemptedRetries <- RetryInput{toRetry, NodeID(nodeID)}
					}
					toForward := append(toRetry, body.Message)
					request := map[string]any{"type": "forward", "messages": toForward}

					_, err := node.SyncRPC(ctx, nodeID, request)
					if err != nil {
						debugLogger.Printf("Failed to sync with %s: %v\n", nodeID, err)
						retries <- RetryInput{toForward, NodeID(nodeID)}
					}
					wg.Done()
				}()
			}
		}
		wg.Wait()
		cancelCtx()
		// }()

		response := map[string]any{"type": "broadcast_ok"}
		return node.Reply(msg, response)
	})
	node.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{"type": "read_ok", "messages": messages}
		return node.Reply(msg, response)
	})
	node.Handle("forward", func(msg maelstrom.Message) error {
		var body ForwardBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, message := range body.Messages {
			inputs <- message
		}

		response := map[string]any{"type": "forward_ok"}
		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
