package pluginhost

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	ex "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/proxyutil"
)

type policyCaller struct {
	host *Host
	url  string
}

func (c *policyCaller) Shutdown() {}
func (c *policyCaller) Call(_ context.Context, _ string, raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	wire, _ := json.Marshal(map[string]any{"host_callback_id": req.HostCallbackID, "method": "GET", "url": c.url})
	out, err := c.host.callHostHTTPDo(context.Background(), wire)
	if err != nil {
		return nil, err
	}
	var envelope pluginabi.Envelope
	if err = json.Unmarshal(out, &envelope); err != nil {
		return nil, err
	}
	var resp pluginapi.HTTPResponse
	if err = json.Unmarshal(envelope.Result, &resp); err != nil {
		return nil, err
	}
	result, _ := json.Marshal(pluginapi.ExecutorResponse{Payload: resp.Body})
	return json.Marshal(pluginabi.Envelope{OK: true, Result: result})
}
func policyServer(t *testing.T, label string) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	hits, conns := new(atomic.Int64), new(atomic.Int64)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); io.WriteString(w, label) }))
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	s.Start()
	t.Cleanup(s.Close)
	return s, hits, conns
}
func TestRPCExecutorPreservesBoundProxy(t *testing.T) {
	for _, globalOn := range []bool{false, true} {
		t.Run(map[bool]string{false: "noGlobal", true: "withGlobal"}[globalOn], func(t *testing.T) {
			origin, _, _ := policyServer(t, "origin")
			account, _, _ := policyServer(t, "account")
			request, _, _ := policyServer(t, "request")
			global, _, _ := policyServer(t, "global")
			h := New()
			t.Cleanup(h.ShutdownAll)
			h.runtimeConfig = &config.Config{}
			if globalOn {
				h.runtimeConfig.ProxyURL = global.URL
			}
			record := &auth.Auth{ID: "synthetic", Provider: "synthetic", ProxyURL: account.URL}
			tr, _, err := proxyutil.BuildHTTPTransport(account.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer tr.CloseIdleConnections()
			ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", tr)
			opts := ex.Options{ProxyURL: request.URL}
			ctx = ex.WithRequestProxyURL(ctx, opts.ProxyURL)
			req := buildExecutorRequest(h, "synthetic", record, ex.Request{}, opts)
			native, err := req.HTTPClient.Do(ctx, pluginapi.HTTPRequest{URL: origin.URL})
			if err != nil {
				t.Fatal(err)
			}
			if string(native.Body) != "request" {
				t.Fatalf("native=%q", native.Body)
			}
			adapter := &rpcPluginAdapter{id: "synthetic", host: h, client: &policyCaller{host: h, url: origin.URL}}
			rpc, err := adapter.Execute(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			want := "request"
			if string(rpc.Payload) != want {
				t.Fatalf("RPC=%q want=%q", rpc.Payload, want)
			}
		})
	}
}
func TestRPCProxyReusesPool(t *testing.T) {
	proxy, hits, conns := policyServer(t, "global")
	h := New()
	t.Cleanup(h.ShutdownAll)
	h.runtimeConfig = &config.Config{}
	h.runtimeConfig.ProxyURL = proxy.URL
	req := buildExecutorRequest(h, "synthetic", nil, ex.Request{}, ex.Options{})
	adapter := &rpcPluginAdapter{id: "synthetic", host: h, client: &policyCaller{host: h, url: "http://synthetic.invalid/"}}
	for i := 0; i < 2; i++ {
		if _, err := adapter.Execute(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if hits.Load() != 2 || conns.Load() != 1 {
		t.Fatalf("hits=%d conns=%d", hits.Load(), conns.Load())
	}
}
func TestRPCBoundClientKeepsCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelOnly", true: "parentDeadline"}[deadline], func(t *testing.T) {
			entered := make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				close(entered)
				<-r.Context().Done()
			}))
			defer s.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if deadline {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 3600000000000)
				defer stop()
			}
			h := New()
			t.Cleanup(h.ShutdownAll)
			h.runtimeConfig = &config.Config{}
			req := buildExecutorRequest(h, "synthetic", nil, ex.Request{}, ex.Options{})
			adapter := &rpcPluginAdapter{id: "synthetic", host: h, client: &policyCaller{host: h, url: s.URL}}
			done := make(chan error, 1)
			go func() { _, err := adapter.Execute(ctx, req); done <- err }()
			<-entered
			h.httpOperations.mu.Lock()
			for _, entry := range h.httpOperations.operations {
				_, got := entry.ctx.Deadline()
				if got != deadline {
					t.Errorf("deadline=%v want=%v", got, deadline)
				}
			}
			h.httpOperations.mu.Unlock()
			cancel()
			if err := <-done; err == nil {
				t.Fatal("cancelled body read succeeded")
			}
			t.Log("callback kept parent's deadline/cancellation; synthesized no new deadline")
		})
	}
}

func TestRPCRefreshPreservesAuthProxy(t *testing.T) {
	origin, oHits, _ := policyServer(t, "origin")
	account, aHits, _ := policyServer(t, "account")
	h := New()
	t.Cleanup(h.ShutdownAll)
	h.runtimeConfig = &config.Config{}
	record := &auth.Auth{ID: "synthetic", Provider: "synthetic", ProxyURL: account.URL}
	req := pluginapi.AuthRefreshRequest{HTTPClient: h.newHTTPClient(record)}
	native, err := req.HTTPClient.Do(context.Background(), pluginapi.HTTPRequest{URL: origin.URL})
	if err != nil {
		t.Fatal(err)
	}
	if string(native.Body) != "account" {
		t.Fatalf("native=%q", native.Body)
	}
	adapter := &rpcPluginAdapter{id: "synthetic", host: h, client: &policyCaller{host: h, url: origin.URL}}
	if _, err := adapter.RefreshAuth(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if aHits.Load() != 2 || oHits.Load() != 0 {
		t.Fatalf("account=%d origin=%d", aHits.Load(), oHits.Load())
	}
}

func TestRPCRequestClientBindings(t *testing.T) {
	h := New()
	t.Cleanup(h.ShutdownAll)
	origin, originHits, _ := policyServer(t, "origin")
	proxy, hits, _ := policyServer(t, "bound")
	client := h.newHTTPClient(&auth.Auth{ProxyURL: proxy.URL})
	a := &rpcPluginAdapter{id: "synthetic", host: h, client: &policyCaller{host: h, url: origin.URL}}
	calls := map[string]func(context.Context) error{
		"models": func(ctx context.Context) error {
			_, err := a.ModelsForAuth(ctx, pluginapi.AuthModelRequest{HTTPClient: client})
			return err
		},
		"loginStart": func(ctx context.Context) error {
			_, err := a.StartLogin(ctx, pluginapi.AuthLoginStartRequest{HTTPClient: client})
			return err
		},
		"loginPoll": func(ctx context.Context) error {
			_, err := a.PollLogin(ctx, pluginapi.AuthLoginPollRequest{HTTPClient: client})
			return err
		},
		"refresh": func(ctx context.Context) error {
			_, err := a.RefreshAuth(ctx, pluginapi.AuthRefreshRequest{HTTPClient: client})
			return err
		},
		"execute": func(ctx context.Context) error {
			_, err := a.Execute(ctx, pluginapi.ExecutorRequest{HTTPClient: client})
			return err
		},
		"tokens": func(ctx context.Context) error {
			_, err := a.CountTokens(ctx, pluginapi.ExecutorRequest{HTTPClient: client})
			return err
		},
		"http": func(ctx context.Context) error {
			_, err := a.HttpRequest(ctx, pluginapi.ExecutorHTTPRequest{HTTPClient: client})
			return err
		},
		"quotaFetch": func(ctx context.Context) error {
			_, err := (rpcQuotaProvider{rpcPluginAdapter: a}).FetchQuota(ctx, pluginapi.QuotaFetchRequest{HTTPClient: client})
			return err
		},
		"quotaReset": func(ctx context.Context) error {
			_, err := (rpcQuotaProvider{rpcPluginAdapter: a}).ResetQuota(ctx, pluginapi.QuotaResetRequest{HTTPClient: client})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			before := hits.Load()
			if err := call(context.Background()); err != nil {
				t.Fatal(err)
			}
			if hits.Load() != before+1 || originHits.Load() != 0 {
				t.Fatal("bound client was lost")
			}
			h.callbackContexts.mu.RLock()
			defer h.callbackContexts.mu.RUnlock()
			if len(h.callbackContexts.contexts) != 0 {
				t.Fatal("callback scope leaked")
			}
		})
	}
}

func TestHostProxyPoolPolicyAndShutdown(t *testing.T) {
	h := New()
	origin, _, _ := policyServer(t, "origin")
	first, _, _ := policyServer(t, "first")
	second, _, _ := policyServer(t, "second")
	for _, setting := range []struct{ request, account, global, want string }{
		{first.URL, second.URL, second.URL, "first"}, {"", first.URL, second.URL, "first"}, {"", "", second.URL, "second"}, {"direct", first.URL, second.URL, "origin"},
	} {
		h.runtimeConfig = &config.Config{}
		h.runtimeConfig.ProxyURL = setting.global
		client := h.newHTTPClientWithProxy(&auth.Auth{ProxyURL: setting.account}, setting.request)
		// Ambient overrides must not replace an explicitly bound request policy.
		ctx := ex.WithRequestProxyURL(context.Background(), "http://127.0.0.1:1")
		resp, err := client.Do(ctx, pluginapi.HTTPRequest{URL: origin.URL})
		if err != nil {
			t.Fatal(err)
		}
		if string(resp.Body) != setting.want {
			t.Fatalf("body=%q want=%q", resp.Body, setting.want)
		}
	}
	if h.httpTransports.Len() != 3 {
		t.Fatalf("pools=%d want=3", h.httpTransports.Len())
	}
	h.ShutdownAll()
	if h.httpTransports.Len() != 0 {
		t.Fatal("shutdown retained idle pools")
	}
}

func TestHostHTTPStreamCallbackUsesBoundClient(t *testing.T) {
	h := New()
	t.Cleanup(h.ShutdownAll)
	origin, oHits, _ := policyServer(t, "origin")
	proxy, hits, _ := policyServer(t, "bound")
	ctx := context.WithValue(context.Background(), callbackHTTPClientKey{}, h.newHTTPClient(&auth.Auth{ProxyURL: proxy.URL}))
	id, closeCallback := h.openCallbackContext(ctx)
	defer closeCallback()
	raw, _ := json.Marshal(map[string]any{"host_callback_id": id, "url": origin.URL})
	wire, err := h.callHostHTTPDoStream(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	var env pluginabi.Envelope
	if err := json.Unmarshal(wire, &env); err != nil {
		t.Fatal(err)
	}
	var resp rpcHostHTTPStreamResponse
	if err := json.Unmarshal(env.Result, &resp); err != nil {
		t.Fatal(err)
	}
	h.httpStreams.close("", nil, resp.StreamID)
	if hits.Load() != 1 || oHits.Load() != 0 {
		t.Fatal("stream callback ignored bound client")
	}
}

func TestRPCExecuteStreamRetainsBoundClientAfterReturning(t *testing.T) {
	h := New()
	t.Cleanup(h.ShutdownAll)
	origin, originHits, _ := policyServer(t, "origin")
	proxy, hits, _ := policyServer(t, "bound")
	plugin := newStreamCallbackPluginClient()
	adapter := &rpcPluginAdapter{id: "stream-plugin", host: h, client: plugin}
	stream, err := adapter.ExecuteStream(context.Background(), pluginapi.ExecutorRequest{Stream: true, HTTPClient: h.newHTTPClient(&auth.Auth{ProxyURL: proxy.URL})})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"host_callback_id": plugin.callbackID, "url": origin.URL})
	if _, err := h.callHostHTTPDo(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 || originHits.Load() != 0 {
		t.Fatal("returning the RPC lost its bound client")
	}
	h.streams.close(plugin.streamID, "")
	for range stream.Chunks {
	}
	if callbackContextExists(h, plugin.callbackID) {
		t.Fatal("completed stream retained its scope")
	}
	if _, err := h.callHostHTTPDo(context.Background(), raw); err == nil {
		t.Fatal("closed scope still accepted HTTP")
	}
}
