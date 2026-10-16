package proxy

import (
	stderrors "errors"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/k3s-io/k3s/pkg/util/errors"

	"github.com/sirupsen/logrus"
)

// lingerTimeout bounds how long Proxy waits for the client to close after the backend has
// finished and the client has been sent EOF.
const lingerTimeout = 30 * time.Second

// Proxy copies data between lconn (the client side, e.g. the hijacked apiserver CONNECT) and
// rconn (the backend, e.g. a remotedialer tunnel to a kubelet) until the backend-to-client
// direction is finished, then closes both.
//
// The backend-to-client direction must be fully drained before closing: a backend that has
// sent its last bytes and closed may still have data buffered on our side (remotedialer keeps
// up to ~4 MiB per connection). The client-to-backend direction must not cut that short: once
// the backend is gone, writes to it fail (io.ErrClosedPipe), and closing lconn at that point
// discards the buffered tail and resets the client.
func Proxy(lconn, rconn io.ReadWriteCloser) error {
	defer rconn.Close()
	defer lconn.Close()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		// client -> backend. If the client goes away (read error/EOF), close both sides so that
		// the return direction ends too, as before: a client that stops sending is not expected
		// to keep reading (half-close from the client side is not supported).
		readErr, writeErr := pipe(rconn, lconn)
		if readErr != nil {
			if !isConnTeardown(readErr) {
				logrus.Warnf("Proxy error: %v", readErr)
			}
			_ = rconn.Close()
			_ = lconn.Close()
			return
		}
		logrus.Debugf("Proxy: backend closed (%v), discarding client data until the return direction drains", writeErr)
		// The backend refused a write: it is already closed. Keep reading (and discarding) what
		// the client sends, e.g. flow-control acknowledgements, while the return direction drains:
		// closing a socket with unread input makes the kernel send a RST instead of a FIN, and
		// the client then loses the data still in flight towards it.
		_, _ = io.Copy(io.Discard, lconn)
	}()

	// backend -> client
	readErr, writeErr := pipe(lconn, rconn)
	if writeErr != nil {
		if !isConnTeardown(writeErr) {
			logrus.Warnf("Proxy error: %v", writeErr)
		}
		return writeErr
	}
	// Backend finished: send EOF to the client and let it read the tail before closing.
	// Wait for the client even if the half-close fails (e.g. TLS close_notify write timeout):
	// closing right away would turn our close into a reset and lose the tail again.
	if cw, ok := lconn.(interface{ CloseWrite() error }); ok {
		switch err := cw.CloseWrite(); {
		case stderrors.Is(err, stderrors.ErrUnsupported):
			// No half-close available: nothing to wait for, close right away.
		default:
			if err != nil {
				logrus.Debugf("Proxy: half-close of client connection failed: %v", err)
			}
			select {
			case <-clientDone:
			case <-time.After(lingerTimeout):
			}
		}
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		if !isConnTeardown(readErr) {
			logrus.Warnf("Proxy error: %v", readErr)
		}
		return readErr
	}
	return nil
}

// isConnTeardown reports errors that are expected when one side closes or resets the connection.
func isConnTeardown(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

// pipe copies src to dst and reports whether it stopped on a read or a write error.
func pipe(dst io.Writer, src io.Reader) (readErr, writeErr error) {
	buff := make([]byte, 1<<15)
	for {
		n, err := src.Read(buff)
		if n > 0 {
			if _, werr := dst.Write(buff[:n]); werr != nil {
				return nil, werr
			}
		}
		if err != nil {
			return err, nil
		}
	}
}
