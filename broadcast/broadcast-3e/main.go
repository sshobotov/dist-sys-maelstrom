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

type Messages struct {
	state []Message
	mutex *sync.Mutex
}

func NewMessages() *Messages {
	return &Messages{[]Message{}, &sync.Mutex{}}
}

func (v *Messages) Add(messages ...Message) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.state = append(v.state, messages...)
}

func (v *Messages) All() []Message {
	return v.state
}

type Forwards struct {
	queue []Message
	mutex *sync.Mutex
}

func NewForwards() *Forwards {
	return &Forwards{[]Message{}, &sync.Mutex{}}
}

func (v *Forwards) Enqueue(message Message) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.queue = append(v.queue, message)
}

func (v *Forwards) DequeueAll() []Message {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	queue := v.queue[:]
	if len(queue) > 0 {
		v.queue = []Message{}
	}
	return queue
}

type Retries struct {
	state  map[NodeID][]Message
	mutex  *sync.Mutex
	logger *log.Logger
}

func NewRetries(logger *log.Logger) *Retries {
	return &Retries{make(map[NodeID][]Message), &sync.Mutex{}, logger}
}

func (v *Retries) Add(input *RetryInput) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.state[input.NodeID] = append(v.state[input.NodeID], input.Messages...)
	v.logger.Printf("Added messages to retry for %s: %v\n", input.NodeID, input.Messages)
}

func (v *Retries) TakeAll(id NodeID) []Message {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	entries := v.state[id]
	if len(entries) > 0 {
		v.state[id] = []Message{}
	}
	return entries
}

func forwardBroadcasts(node *maelstrom.Node, queue *Forwards, retries *Retries, logger *log.Logger) {
	fromQueue := queue.DequeueAll()
	if len(fromQueue) < 1 {
		return
	}
	logger.Printf("Going to forward %d messages", len(fromQueue))

	wg := sync.WaitGroup{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 250*time.Millisecond)
	for _, nodeID := range node.NodeIDs() {
		if nodeID != node.ID() {
			wg.Add(1)
			go func() {
				toForward := append(fromQueue, retries.TakeAll(NodeID(nodeID))...)
				request := map[string]any{"type": "forward", "messages": toForward}

				_, err := node.SyncRPC(ctx, nodeID, request)
				if err != nil {
					logger.Printf("Failed to sync with %s: %v\n", nodeID, err)
					retries.Add(&RetryInput{toForward, NodeID(nodeID)})
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
	cancelCtx()
}

// 25 nodes grid -> 2 * sqrt(n) = 10 network delays
// 100ms latency, 1s for p50, 2s for p99
// <20 per broadcast -> batching
// rate=100 -> ~4 broadcasts per second per node
func main() {
	node := maelstrom.NewNode()
	debugLogger := log.New(os.Stderr, "DEBUG ", log.LstdFlags)

	messages := NewMessages()

	forwards := NewForwards()
	retries := NewRetries(debugLogger)
	schedule := time.NewTicker(1500 * time.Millisecond)

	go func() {
		for range schedule.C {
			forwardBroadcasts(node, forwards, retries, debugLogger)
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

		messages.Add(body.Message)
		forwards.Enqueue(body.Message)

		response := map[string]any{"type": "broadcast_ok"}
		return node.Reply(msg, response)
	})
	node.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{"type": "read_ok", "messages": messages.All()}
		return node.Reply(msg, response)
	})
	node.Handle("forward", func(msg maelstrom.Message) error {
		var body ForwardBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, message := range body.Messages {
			messages.Add(message)
		}

		response := map[string]any{"type": "forward_ok"}
		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
