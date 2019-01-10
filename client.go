package eventsource

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// Client wraps an http connection and converts it to an
// event stream.
type Client struct {
	flush     http.Flusher
	write     io.Writer
	ctx       context.Context
	events    chan Event
	closed    bool
	waiter    sync.WaitGroup
	lock      sync.Mutex
	lastFlush uint64
	lastWrite uint64
}

// NewClient creates a client wrapping a response writer.
// The response writer must support http.Flusher interface.
// When writing, the client will automatically send some headers. Passing the
// original http.Request helps determine which headers, but the request it is
// optional.
// Returns nil on error.
func NewClient(w http.ResponseWriter, req *http.Request) *Client {
	c := &Client{
		events: make(chan Event, 100),
		write:  w,
	}

	// Check to ensure we support flushing
	flush, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	c.flush = flush

	c.ctx = req.Context()

	// Send the initial headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	if req == nil || req.ProtoMajor < 2 {
		w.Header().Set("Connection", "keep-alive")
	}
	flush.Flush()

	// start the sending thread
	c.waiter.Add(2)
	go c.run()
	go c.flusher()
	return c
}

// Send queues an event to be sent to the client.
// This does not block until the event has been sent,
// however it could block if the event queue is full.
// Returns an error if the Client has disconnected
func (c *Client) Send(ev Event) error {
	if c.closed {
		return io.ErrClosedPipe
	}
	c.events <- ev
	return nil
}

// Send queues an event to be sent to the client.
// This guarantees not block until the event has been sent.
// Returns true if blocked
// Returns an error if the Client has disconnected
func (c *Client) SendNonBlocking(ev Event) (bool, error) {
	if c.closed {
		return false, io.ErrClosedPipe
	}
	select {
	case c.events <- ev:
	default:
		return true, nil
	}
	return false, nil
}

// Shutdown terminates a client connection
func (c *Client) Shutdown() {
	close(c.events)
	c.waiter.Wait()
}

// Wait blocks and waits for the client to be shutdown.
// Call this in http handler threads to prevent the server from closing
// the client connection.
func (c *Client) Wait() {
	c.waiter.Wait()
}

// Worker thread for the client responsible for writing events
func (c *Client) run() {
	done := c.ctx.Done()
	for {
		select {
		case ev, ok := <-c.events:
			// check for shutdown
			if !ok {
				c.closed = true
				c.waiter.Done()
				return
			}

			// send the event
			c.lock.Lock()
			io.Copy(c.write, &ev)
			c.lastWrite += 1
			c.lock.Unlock()

		case <-done:
			c.closed = true
			c.waiter.Done()
			return
		}

	}
}

// flusher amortizes flushing costs for high activity SSE channels
func (c *Client) flusher() {
	for {
		time.Sleep(100 * time.Millisecond)
		c.lock.Lock()
		if c.closed {
			break
		}
		if c.lastFlush < c.lastWrite {
			c.lastFlush = c.lastWrite
			c.flush.Flush()
		}
		c.lock.Unlock()
	}

	c.waiter.Done()
}
