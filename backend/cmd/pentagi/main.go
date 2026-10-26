package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testWaitTimeout     = 10 * time.Second
	testShutdownTimeout = 15 * time.Second
	testDrainTimeout    = 5 * time.Second
)

var errSimulatedServerFailure = errors.New("accept tcp: too many open files")

type shutdownRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *shutdownRecorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)
}

func (r *shutdownRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.events...)
}

func requireEvents(t *testing.T, got, want []string) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

type ctxSnapshot struct {
	err         error
	deadline    time.Time
	hasDeadline bool
	taken       time.Time
}

// Snapshot at call time: production code cancels these contexts on return.
func snapshotCtx(ctx context.Context) ctxSnapshot {
	deadline, ok := ctx.Deadline()

	return ctxSnapshot{err: ctx.Err(), deadline: deadline, hasDeadline: ok, taken: time.Now()}
}

func (c ctxSnapshot) requireFresh(t *testing.T, name string, limit time.Duration) {
	t.Helper()

	if c.err != nil {
		t.Errorf("%s context was already done: %v", name, c.err)
	}
	if !c.hasDeadline {
		t.Errorf("%s context has no deadline", name)
		return
	}
	if remaining := c.deadline.Sub(c.taken); remaining <= 0 || remaining > limit {
		t.Errorf("%s deadline is %v away, want within (0, %v]", name, remaining, limit)
	}
}

type stubHTTPServer struct {
	shutdownFn func(ctx context.Context) error
	closeFn    func() error
}

func (s *stubHTTPServer) Shutdown(ctx context.Context) error { return s.shutdownFn(ctx) }
func (s *stubHTTPServer) Close() error                       { return s.closeFn() }

func newGracefulStub(emit func(string)) (srv *stubHTTPServer, serve func() error, shutdownCtx *ctxSnapshot) {
	stopped := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(stopped) }) }

	shutdownCtx = &ctxSnapshot{}
	srv = &stubHTTPServer{
		shutdownFn: func(ctx context.Context) error {
			*shutdownCtx = snapshotCtx(ctx)
			emit("shutdown")
			stop()
			return nil
		},
		closeFn: func() error {
			emit("close")
			stop()
			return nil
		},
	}
	serve = func() error {
		<-stopped
		return http.ErrServerClosed
	}

	return srv, serve, shutdownCtx
}

type spyHTTPServer struct {
	*http.Server

	shutdownStarted chan struct{}
	startOnce       sync.Once
	closeCalls      atomic.Int32
}

func (s *spyHTTPServer) Shutdown(ctx context.Context) error {
	s.startOnce.Do(func() { close(s.shutdownStarted) })
	return s.Server.Shutdown(ctx)
}

func (s *spyHTTPServer) Close() error {
	s.closeCalls.Add(1)
	return s.Server.Close()
}

type blockedHandler struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (h *blockedHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.enterOnce.Do(func() { close(h.entered) })
	<-h.release
	_, _ = io.WriteString(w, "done")
}

func (h *blockedHandler) unblock() { h.releaseOnce.Do(func() { close(h.release) }) }

func startBlockedServer(t *testing.T) (spy *spyHTTPServer, serve func() error, h *blockedHandler, url string) {
	t.Helper()

	h = &blockedHandler{entered: make(chan struct{}), release: make(chan struct{})}
	ts := httptest.NewUnstartedServer(h)

	t.Cleanup(ts.Close)
	t.Cleanup(h.unblock)

	spy = &spyHTTPServer{Server: ts.Config, shutdownStarted: make(chan struct{})}
	serve = func() error { return ts.Config.Serve(ts.Listener) }

	return spy, serve, h, "http://" + ts.Listener.Addr().String()
}

type requestResult struct {
	status int
	body   string
	err    error
}

func startRequest(url string) <-chan requestResult {
	result := make(chan requestResult, 1)

	go func() {
		client := &http.Client{
			Timeout:   testWaitTimeout,
			Transport: &http.Transport{DisableKeepAlives: true},
		}

		resp, err := client.Get(url)
		if err != nil {
			result <- requestResult{err: err}
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		result <- requestResult{status: resp.StatusCode, body: string(body), err: err}
	}()

	return result
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(testWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitError(t *testing.T, ch <-chan error, what string) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(testWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func waitResult(t *testing.T, ch <-chan requestResult, what string) requestResult {
	t.Helper()

	select {
	case res := <-ch:
		return res
	case <-time.After(testWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return requestResult{}
	}
}

func TestMain_Timeouts_MatchContract(t *testing.T) {
	shutdown, drain := httpShutdownTimeout, telemetryDrainTimeout

	if shutdown != 15*time.Second {
		t.Errorf("httpShutdownTimeout = %v, want 15s", shutdown)
	}
	if drain != 5*time.Second {
		t.Errorf("telemetryDrainTimeout = %v, want 5s", drain)
	}
}

func TestMain_SuperviseHTTPServer_ActiveRequestCompletes(t *testing.T) {
	spy, serve, handler, url := startBlockedServer(t)

	ctx, sendSignal := context.WithCancel(context.Background())
	defer sendSignal()

	done := make(chan error, 1)
	go func() { done <- superviseHTTPServer(ctx, spy, serve, testShutdownTimeout) }()

	response := startRequest(url)
	waitClosed(t, handler.entered, "the request to reach the handler")

	sendSignal()
	waitClosed(t, spy.shutdownStarted, "Shutdown to be called")

	select {
	case err := <-done:
		t.Fatalf("shutdown returned while a request was in flight: %v", err)
	default:
	}

	handler.unblock()

	if err := waitError(t, done, "graceful shutdown to finish"); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}

	res := waitResult(t, response, "the in-flight response")
	if res.err != nil || res.status != http.StatusOK || res.body != "done" {
		t.Fatalf("in-flight request = (status %d, body %q, err %v), want it to complete normally",
			res.status, res.body, res.err)
	}

	if n := spy.closeCalls.Load(); n != 0 {
		t.Errorf("Close called %d times after a successful graceful shutdown, want 0", n)
	}
}

func TestMain_SuperviseHTTPServer_DeadlineExpiryForcesClose(t *testing.T) {
	spy, serve, handler, url := startBlockedServer(t)

	ctx, sendSignal := context.WithCancel(context.Background())
	defer sendSignal()

	done := make(chan error, 1)
	go func() { done <- superviseHTTPServer(ctx, spy, serve, 100*time.Millisecond) }()

	response := startRequest(url)
	waitClosed(t, handler.entered, "the request to reach the handler")

	sendSignal()

	err := waitError(t, done, "shutdown to give up")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to report context.DeadlineExceeded", err)
	}

	if n := spy.closeCalls.Load(); n != 1 {
		t.Errorf("Close called %d times, want exactly 1 forced close", n)
	}

	if res := waitResult(t, response, "the stuck request to be cut"); res.err == nil {
		t.Errorf("stuck request finished with status %d, want its connection closed", res.status)
	}
}

func TestMain_ShutdownHTTPServer_ForceCloseErrorIsReported(t *testing.T) {
	closeErr := errors.New("close failed")
	srv := &stubHTTPServer{
		shutdownFn: func(context.Context) error { return context.DeadlineExceeded },
		closeFn:    func() error { return closeErr },
	}

	err := shutdownHTTPServer(srv, testShutdownTimeout)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to keep the graceful-shutdown failure", err)
	}
	if !errors.Is(err, closeErr) {
		t.Errorf("err = %v, want it to include the forced-close failure", err)
	}
}

func TestMain_SuperviseHTTPServer_ErrServerClosedAfterShutdownIsExpected(t *testing.T) {
	rec := &shutdownRecorder{}
	srv, serve, shutdownCtx := newGracefulStub(rec.add)

	ctx, sendSignal := context.WithCancel(context.Background())

	if err := superviseHTTPServer(ctx, srv, serve, testShutdownTimeout); err != nil {
		t.Fatalf("http.ErrServerClosed after a requested shutdown must not be an error, got: %v", err)
	}

	requireEvents(t, rec.snapshot(), []string{"shutdown"})
	shutdownCtx.requireFresh(t, "Shutdown", testShutdownTimeout)
}

func TestMain_SuperviseHTTPServer_ErrServerClosedWithoutShutdownIsUnexpected(t *testing.T) {
	rec := &shutdownRecorder{}
	srv, _, _ := newGracefulStub(rec.add)
	serve := func() error { return http.ErrServerClosed }

	err := superviseHTTPServer(context.Background(), srv, serve, testShutdownTimeout)

	if !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("err = %v, want the unrequested closure reported", err)
	}
}

func TestMain_SuperviseHTTPServer_ServerErrorRacingSignalIsNotLost(t *testing.T) {
	stopped := make(chan struct{})
	srv := &stubHTTPServer{
		shutdownFn: func(context.Context) error { close(stopped); return nil },
		closeFn:    func() error { return nil },
	}
	serve := func() error {
		<-stopped
		return errSimulatedServerFailure
	}

	ctx, sendSignal := context.WithCancel(context.Background())
	sendSignal()

	err := superviseHTTPServer(ctx, srv, serve, testShutdownTimeout)

	if !errors.Is(err, errSimulatedServerFailure) {
		t.Fatalf("err = %v, want the genuine server error reported", err)
	}
}

func TestMain_SuperviseHTTPServer_UnexpectedFailureIsReportedAndServerStopped(t *testing.T) {
	rec := &shutdownRecorder{}
	srv, _, shutdownCtx := newGracefulStub(rec.add)
	serve := func() error { return errSimulatedServerFailure }

	err := superviseHTTPServer(context.Background(), srv, serve, testShutdownTimeout)

	if !errors.Is(err, errSimulatedServerFailure) {
		t.Fatalf("err = %v, want the server failure propagated", err)
	}

	requireEvents(t, rec.snapshot(), []string{"shutdown"})
	shutdownCtx.requireFresh(t, "Shutdown", testShutdownTimeout)
}

func TestMain_ServeUntilShutdown_SignalShutsDownThenDrains(t *testing.T) {
	rec := &shutdownRecorder{}
	srv, serve, _ := newGracefulStub(rec.add)

	var drainCtx ctxSnapshot
	drain := func(ctx context.Context) error {
		drainCtx = snapshotCtx(ctx)
		rec.add("drain")
		return nil
	}

	ctx, sendSignal := context.WithCancel(context.Background())
	sendSignal()

	if code := serveUntilShutdown(ctx, srv, serve, testShutdownTimeout, drain); code != 0 {
		t.Fatalf("exit code = %d, want 0 after a clean signal shutdown", code)
	}

	requireEvents(t, rec.snapshot(), []string{"shutdown", "drain"})
	drainCtx.requireFresh(t, "telemetry drain", testDrainTimeout)
}

func TestMain_ServeUntilShutdown_DrainFailureDoesNotChangeExitCode(t *testing.T) {
	rec := &shutdownRecorder{}
	srv, serve, _ := newGracefulStub(rec.add)
	drain := func(context.Context) error { return errors.New("collector unreachable") }

	ctx, sendSignal := context.WithCancel(context.Background())
	sendSignal()

	if code := serveUntilShutdown(ctx, srv, serve, testShutdownTimeout, drain); code != 0 {
		t.Fatalf("exit code = %d, want 0: an incomplete drain is only logged", code)
	}
}

func failureScenario(emit func(string)) int {
	defer emit("cleanup")

	srv := &stubHTTPServer{
		shutdownFn: func(context.Context) error { emit("shutdown"); return nil },
		closeFn:    func() error { emit("close"); return nil },
	}
	serve := func() error { return errSimulatedServerFailure }
	drain := func(context.Context) error { emit("drain"); return nil }

	return serveUntilShutdown(context.Background(), srv, serve, testShutdownTimeout, drain)
}

func TestMain_ServeUntilShutdown_UnexpectedFailureCleansUpAndReturnsFailure(t *testing.T) {
	rec := &shutdownRecorder{}

	code := failureScenario(rec.add)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	requireEvents(t, rec.snapshot(), []string{"shutdown", "drain", "cleanup"})
}

func TestMain_ServeUntilShutdown_ListenFailureOnRealServer(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer occupied.Close()

	srv := &http.Server{Addr: occupied.Addr().String(), Handler: http.NotFoundHandler()}

	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()

	drained := false
	drain := func(context.Context) error { drained = true; return nil }

	code := serveUntilShutdown(ctx, srv, srv.ListenAndServe, testShutdownTimeout, drain)

	if code != 1 {
		t.Errorf("exit code = %d, want 1 when the address is already in use", code)
	}
	if !drained {
		t.Error("telemetry was not drained after the listener failed")
	}
}

const failureHelperEnv = "PENTAGI_TEST_SERVER_FAILURE_HELPER"

// Child process of TestMain_ServeUntilShutdown_UnexpectedFailureExitStatus.
func TestMain_ServeUntilShutdown_FailureHelperProcess(t *testing.T) {
	if os.Getenv(failureHelperEnv) != "1" {
		t.Skip("helper process for TestMain_ServeUntilShutdown_UnexpectedFailureExitStatus")
	}

	os.Exit(failureScenario(func(event string) { fmt.Println("EVENT:" + event) }))
}

// runApplication itself needs PostgreSQL and Docker, so a stand-in with the same shape runs instead.
func TestMain_ServeUntilShutdown_UnexpectedFailureExitStatus(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestMain_ServeUntilShutdown_FailureHelperProcess$")
	cmd.Env = append(os.Environ(), failureHelperEnv+"=1")

	out, err := cmd.Output()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("child err = %v, want a non-zero exit\nstdout:\n%s", err, out)
	}
	if got := exitErr.ExitCode(); got != 1 {
		t.Fatalf("child exit status = %d, want 1\nstdout:\n%s\nstderr:\n%s", got, out, exitErr.Stderr)
	}

	var events []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(line, "EVENT:") {
			events = append(events, strings.TrimPrefix(line, "EVENT:"))
		}
	}

	requireEvents(t, events, []string{"shutdown", "drain", "cleanup"})
}
