package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/ai-crypto-onramp/blockchain-gateway/internal/app"
)

// buildTestServer builds the app with defaults and wraps it in a test
// server. The caller is responsible for Close.
func buildTestServer() (*httptest.Server, error) {
	cfg := app.LoadConfig()
	cfg.Port = "0"
	srv, err := app.Build(cfg)
	if err != nil {
		return nil, err
	}
	ts := httptest.NewServer(srv.HTTPHandler())
	return ts, nil
}

func buildTestServerOnPort(port string) (*app.Server, error) {
	cfg := app.LoadConfig()
	cfg.Port = port
	return app.Build(cfg)
}

func newHTTPServerOnPort(port string) *http.Server {
	return &http.Server{Addr: ":" + port, Handler: http.NewServeMux()}
}

func netListen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

func portFromAddr(addr string) string {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return p
}

// guard against unused helpers if a test is removed.
var _ = fmt.Sprintf
var _ = strconv.Itoa
var _ = strings.Split