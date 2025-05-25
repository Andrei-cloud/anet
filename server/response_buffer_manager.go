package server

import (
	"sync"
	"time"

	"github.com/andrei-cloud/anet"
)

// responseBufferManager manages response buffers using size-specific ring buffers.
type responseBufferManager struct {
	smallBuffers  *anet.RingBuffer[[]byte] // < 1KB responses
	mediumBuffers *anet.RingBuffer[[]byte] // 1KB - 8KB responses
	largeBuffers  *anet.RingBuffer[[]byte] // 8KB - 64KB responses
	mu            sync.RWMutex             // protects buffer operations
}

// newResponseBufferManager creates a new buffer manager.
func newResponseBufferManager() *responseBufferManager {
	return &responseBufferManager{
		smallBuffers:  anet.NewRingBuffer[[]byte](32), // 32 small buffers
		mediumBuffers: anet.NewRingBuffer[[]byte](16), // 16 medium buffers
		largeBuffers:  anet.NewRingBuffer[[]byte](8),  // 8 large buffers
	}
}

// getBuffer retrieves an appropriately sized buffer.
func (rbm *responseBufferManager) getBuffer(required int) []byte {
	rbm.mu.RLock()
	defer rbm.mu.RUnlock()

	switch {
	case required <= 1024:
		if buf, ok := rbm.smallBuffers.Dequeue(); ok && cap(buf) >= required {
			return buf[:required]
		}

		return make([]byte, required, 1024)

	case required <= 8192:
		if buf, ok := rbm.mediumBuffers.Dequeue(); ok && cap(buf) >= required {
			return buf[:required]
		}

		return make([]byte, required, 8192)

	case required <= 65536:
		if buf, ok := rbm.largeBuffers.Dequeue(); ok && cap(buf) >= required {
			return buf[:required]
		}

		return make([]byte, required, 65536)

	default:
		// For very large buffers, don't pool them.
		return make([]byte, required)
	}
}

// returnBuffer returns a buffer to the appropriate ring buffer.
func (rbm *responseBufferManager) returnBuffer(buf []byte) {
	if cap(buf) > 65536 {
		return // Don't pool very large buffers.
	}

	rbm.mu.Lock()
	defer rbm.mu.Unlock()

	// Reset buffer length to capacity for reuse.
	buf = buf[:cap(buf)]

	switch cap(buf) {
	case 1024:
		rbm.smallBuffers.Enqueue(buf)
	case 8192:
		rbm.mediumBuffers.Enqueue(buf)
	case 65536:
		rbm.largeBuffers.Enqueue(buf)
	}
}

// Enhanced dispatchMessage with optimized buffer management.
func (s *Server) dispatchMessageOptimized(sc *ServerConn, taskID, request []byte) {
	go func() {
		resp, err := s.handler.HandleMessage(sc, request)
		if err != nil {
			s.logf("handler error: %v", err)
		}

		if resp == nil {
			return
		}

		// Use size-specific buffer management.
		required := len(taskID) + len(resp)
		buf := s.responseBuffers.getBuffer(required)

		copy(buf[:len(taskID)], taskID)
		copy(buf[len(taskID):], resp)

		sc.writeMu.Lock()
		if s.config.WriteTimeout > 0 {
			if err := sc.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
				s.logf("set write deadline error: %v", err)
			}
		}

		writeErr := anet.Write(sc.Conn, buf)
		sc.writeMu.Unlock()

		// Return buffer to pool.
		s.responseBuffers.returnBuffer(buf)

		if writeErr != nil {
			s.logf("write error: %v", writeErr)
			if err := sc.Conn.Close(); err != nil {
				s.logf("connection close error: %v", err)
			}
		}
	}()
}
