package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Eomaxl/gossip-go/internal/gossip"
)

func newTestGossiper(t *testing.T) *gossip.Gossiper {
	t.Helper()
	g, err := gossip.New(gossip.Config{
		Bind:    "127.0.0.1:0",
		Cluster: "http-test",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("gossip.New: %v", err)
	}
	t.Cleanup(g.Stop)
	return g
}

// doRequest drives routes(g) directly through ServeHTTP, without a real
// listening socket — deterministic, and lets us hand the handler a body
// reader with exact control over how many bytes each Read() call returns.
func doRequest(g *gossip.Gossiper, method, target string, body io.Reader) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, body)
	w := httptest.NewRecorder()
	routes(g).ServeHTTP(w, req)
	return w
}

func TestHealthz(t *testing.T) {
	w := doRequest(newTestGossiper(t), http.MethodGet, "/healthz", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != "ok\n" {
		t.Errorf("body = %q, want %q", w.Body.String(), "ok\n")
	}
}

func TestMembersEndpoint(t *testing.T) {
	g := newTestGossiper(t)
	g.SetAppState(gossip.AppDC, "DC1")

	w := doRequest(g, http.MethodGet, "/members", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var members []gossip.MemberView
	if err := json.Unmarshal(w.Body.Bytes(), &members); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("got %d members, want 1", len(members))
	}
	if !members[0].Self || !members[0].Live {
		t.Errorf("self entry wrong: %+v", members[0])
	}
	if members[0].AppState["DC"] != "DC1" {
		t.Errorf("DC = %q, want DC1", members[0].AppState["DC"])
	}
}

func TestStatsEndpoint(t *testing.T) {
	g := newTestGossiper(t)

	w := doRequest(g, http.MethodGet, "/stats", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var stats gossip.Stats
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.Self != g.Self() {
		t.Errorf("Self = %q, want %q", stats.Self, g.Self())
	}
	if stats.Cluster != "http-test" {
		t.Errorf("Cluster = %q, want http-test", stats.Cluster)
	}
	if stats.Known != 1 || stats.LiveCount != 1 {
		t.Errorf("Known/LiveCount = %d/%d, want 1/1", stats.Known, stats.LiveCount)
	}
}

func TestPutState_SetsAndPropagatesToMembers(t *testing.T) {
	g := newTestGossiper(t)

	w := doRequest(g, http.MethodPut, "/state/FOO", strings.NewReader("bar"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var got map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["key"] != "FOO" || got["value"] != "bar" {
		t.Errorf("response = %+v, want key=FOO value=bar", got)
	}

	members := g.Members()
	if members[0].AppState["FOO"] != "bar" {
		t.Errorf("state did not reach Members(): %+v", members[0].AppState)
	}
}

func TestPutState_TrimsWhitespace(t *testing.T) {
	g := newTestGossiper(t)
	w := doRequest(g, http.MethodPut, "/state/FOO", strings.NewReader("  bar  \n"))
	var got map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["value"] != "bar" {
		t.Errorf("value = %q, want trimmed %q", got["value"], "bar")
	}
}

func TestPutState_EmptyBodyRejected(t *testing.T) {
	g := newTestGossiper(t)
	w := doRequest(g, http.MethodPut, "/state/FOO", strings.NewReader(""))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestPutState_MissingKeySegment404s(t *testing.T) {
	g := newTestGossiper(t)
	// The route pattern "/state/{key}" never matches an empty last path
	// segment, so this 404s at the mux; the handler's own "missing key" guard
	// is defensive and effectively unreachable through this route.
	w := doRequest(g, http.MethodPut, "/state/", strings.NewReader("bar"))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestPutState_BodyOverLimitRejected(t *testing.T) {
	g := newTestGossiper(t)
	oversized := strings.Repeat("x", 4097)
	w := doRequest(g, http.MethodPut, "/state/FOO", strings.NewReader(oversized))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for a body over the 4096-byte limit", w.Code)
	}
}

func TestPutState_BodyAtLimitAccepted(t *testing.T) {
	g := newTestGossiper(t)
	exact := strings.Repeat("x", 4096)
	w := doRequest(g, http.MethodPut, "/state/FOO", strings.NewReader(exact))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a body exactly at the limit: %s", w.Code, w.Body.String())
	}
}

// Regression test: the handler used to read the body with a single
// Body.Read(buf) call, which only returns whatever one read produced and
// silently truncates the rest when the body arrives in more than one chunk.
func TestPutState_LargeValueIsNotTruncatedAcrossChunkBoundaries(t *testing.T) {
	g := newTestGossiper(t)
	value := strings.Repeat("y", 3000)

	req := httptest.NewRequest(http.MethodPut, "/state/BIG", nil)
	req.Body = io.NopCloser(&slowBodyReader{chunks: chunkString(value, 500)})
	w := httptest.NewRecorder()
	routes(g).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var got map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got["value"]) != len(value) {
		t.Fatalf("value length = %d, want %d (looks truncated)", len(got["value"]), len(value))
	}
	if got["value"] != value {
		t.Error("value content corrupted across chunk boundaries")
	}
}

// slowBodyReader delivers its content one pre-sized chunk per Read() call —
// the same shape a body split across several TCP segments takes, where a
// single Read() only returns whatever arrived so far.
type slowBodyReader struct {
	chunks [][]byte
	i      int
}

func (s *slowBodyReader) Read(p []byte) (int, error) {
	if s.i >= len(s.chunks) {
		return 0, io.EOF
	}
	n := copy(p, s.chunks[s.i])
	s.chunks[s.i] = s.chunks[s.i][n:]
	if len(s.chunks[s.i]) == 0 {
		s.i++
	}
	return n, nil
}

func chunkString(s string, size int) [][]byte {
	var out [][]byte
	b := []byte(s)
	for len(b) > 0 {
		n := size
		if n > len(b) {
			n = len(b)
		}
		out = append(out, b[:n])
		b = b[n:]
	}
	return out
}
