package proxy

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

// failConn fails every Read and Write with err.
type failConn struct{ err error }

func (c failConn) Read([]byte) (int, error)  { return 0, c.err }
func (c failConn) Write([]byte) (int, error) { return 0, c.err }
func (failConn) Close() error                { return nil }

// Test_UnitProxyBothDirectionsFail checks that no pipe goroutine stays blocked when both directions fail.
func Test_UnitProxyBothDirectionsFail(t *testing.T) {
	g := gomega.NewWithT(t)
	out := logrus.StandardLogger().Out
	logrus.SetOutput(t.Output())
	t.Cleanup(func() { logrus.SetOutput(out) })

	conn := failConn{errors.New("i/o error")}
	before := runtime.NumGoroutine()
	for range 10000 {
		_ = Proxy(conn, conn)
	}
	g.Eventually(runtime.NumGoroutine, 5*time.Second).Should(gomega.BeNumerically("<=", before))
}

// fakeTunnel mimics a remotedialer connection as seen by the supervisor: the backend (kubelet)
// has already sent everything and closed; the data is still in the read buffer and Read drains
// it before returning EOF (remotedialer readbuffer.go). Once the close has been received, Write
// returns io.ErrClosedPipe (remotedialer connection.go).
type fakeTunnel struct {
	mu   sync.Mutex
	data *bytes.Reader
}

func (f *fakeTunnel) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data.Read(p)
}
func (f *fakeTunnel) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (f *fakeTunnel) Close() error              { return nil }

// Test_UnitProxySlowClientGetsWholeStream: the client (apiserver) reads slowly and keeps writing
// a few bytes towards the backend (stream flow-control acknowledgements). Every byte buffered
// from the backend must reach the client.
func Test_UnitProxySlowClientGetsWholeStream(t *testing.T) {
	const total = 4 << 20 // ~2x remotedialer MaxBuffer
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		srv, err := ln.Accept()
		if err != nil {
			return
		}
		_ = Proxy(srv, &fakeTunnel{data: bytes.NewReader(make([]byte, total))})
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	_ = cli.(*net.TCPConn).SetReadBuffer(64 << 10)

	got := 0
	buf := make([]byte, 32<<10)
	_ = cli.SetDeadline(time.Now().Add(20 * time.Second))
	for {
		_, _ = cli.Write([]byte{0}) // flow-control acknowledgement
		n, err := cli.Read(buf)
		got += n
		if err != nil {
			if err != io.EOF {
				t.Logf("read: %v", err)
			}
			break
		}
		time.Sleep(2 * time.Millisecond) // reader slower than the tunnel
	}
	if got != total {
		t.Fatalf("stream truncated: got %d of %d bytes (%d lost)", got, total, total-got)
	}
}

// blockingTunnel is a live backend: Read blocks until Close.
type blockingTunnel struct{ closed chan struct{} }

func (b *blockingTunnel) Read([]byte) (int, error) { <-b.closed; return 0, io.EOF }
func (b *blockingTunnel) Write(p []byte) (int, error) {
	select {
	case <-b.closed:
		return 0, io.ErrClosedPipe
	default:
		return len(p), nil
	}
}
func (b *blockingTunnel) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

// Test_UnitProxyClientClosesFirst: if the client goes away first, Proxy closes the backend and returns.
func Test_UnitProxyClientClosesFirst(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	bt := &blockingTunnel{closed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		srv, err := ln.Accept()
		if err != nil {
			return
		}
		_ = Proxy(srv, bt)
		close(done)
	}()
	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cli.Write([]byte("hello"))
	_ = cli.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not return after the client closed")
	}
	select {
	case <-bt.closed:
	default:
		t.Fatal("backend not closed")
	}
}

// noHalfClose wraps a net.Conn and reports half-close as unsupported, like
// connReadWriteCloser does for connections that cannot half-close.
type noHalfClose struct{ net.Conn }

func (noHalfClose) CloseWrite() error { return errors.ErrUnsupported }

// Test_UnitProxyNoHalfCloseDoesNotLinger: without half-close support, Proxy must not linger once the backend is done.
func Test_UnitProxyNoHalfCloseDoesNotLinger(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan time.Duration, 1)
	go func() {
		srv, err := ln.Accept()
		if err != nil {
			return
		}
		start := time.Now()
		_ = Proxy(noHalfClose{srv}, &fakeTunnel{data: bytes.NewReader([]byte("ok"))})
		done <- time.Since(start)
	}()
	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	select {
	case d := <-done:
		if d > 5*time.Second {
			t.Fatalf("Proxy lingered %v without half-close support", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Proxy lingered without half-close support")
	}
}
