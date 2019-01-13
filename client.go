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
	flusher  http.Flusher
	write    io.Writer
	ctx      context.Context
	events   chan Event
	closed   bool
	waiter   sync.WaitGroup
	lock     sync.Mutex
	flushing *time.Timer
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
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	c.flusher = flusher

	c.ctx = req.Context()

	// Send the initial headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	if req == nil || req.ProtoMajor < 2 {
		w.Header().Set("Connection", "keep-alive")
	}
	flusher.Flush()

	// start the sending thread
	c.waiter.Add(1)
	go c.run()
	return c
}

func (c *Client) Closed() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.closed
}

// Send queues an event to be sent to the client.
// This does not block until the event has been sent,
// however it could block if the event queue is full.
// Returns an error if the Client has disconnected
func (c *Client) Send(ev Event) error {
	if c.Closed() {
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
	if c.Closed() {
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
				c.lock.Lock()
				c.closed = true
				c.lock.Unlock()
				c.waiter.Done()
				return
			}

			// send the event
			c.lock.Lock()
			io.Copy(c.write, &ev)
			if c.flushing == nil {
				c.flushing = time.AfterFunc(100*time.Millisecond, c.flush)
			}
			c.lock.Unlock()

		case <-done:
			c.lock.Lock()
			c.closed = true
			c.lock.Unlock()
			c.waiter.Done()
			return
		}

	}
}

// flusher amortizes flushing costs for high activity SSE channels
func (c *Client) flush() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed || c.ctx.Err() != nil {
		return
	}
	c.flushing = nil
	c.flusher.Flush()
}
