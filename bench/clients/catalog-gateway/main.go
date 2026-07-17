// catalog-gateway is a minimal, spec-conformant OAuth2 gateway that fronts a
// real Iceberg REST catalog (Lakekeeper) and requires bearer authentication on
// every catalog data/metadata call. It exists to prove — end to end, against a
// server that genuinely rejects unauthenticated requests — that icegres can
// serve a non-Lakekeeper-shaped catalog through the OAuth2 auth surface added
// in P6/B1 (the `token`, `credential`, `oauth2-server-uri`, `scope` props).
//
// It is deliberately a FALLBACK, not a second Iceberg implementation: Apache
// Polaris cannot be built on this box (its Gradle 9.6.1 wrapper download is
// denied by the agent proxy). What this DOES faithfully implement is the exact
// OAuth2 client-credentials wire flow the pinned iceberg-rust 0.9.1 REST client
// speaks, plus real bearer enforcement. See docs/catalog-support.md — the proof
// is labeled by-construction / spec-conformant-auth-harness, never "proven
// against Polaris".
//
// Stdlib only (net/http + net/http/httputil.ReverseProxy). No dependencies.
//
// Endpoints:
//
//	POST /v1/oauth/tokens   OPEN. Accepts application/x-www-form-urlencoded
//	                        grant_type=client_credentials + client_id +
//	                        client_secret (+ optional scope). Validates a fixed
//	                        client, mints a random opaque bearer, and returns
//	                        {"access_token","token_type":"Bearer","expires_in"}.
//	                        Bad/absent credentials => 401.
//	GET  .../v1/config      OPEN. The Iceberg REST discovery handshake that
//	                        advertises the token endpoint; every client must be
//	                        able to reach it before it holds a token.
//	everything else         Requires `Authorization: Bearer <minted>`; 401
//	                        otherwise (the WWW-Authenticate challenge redacts).
//	                        Valid tokens reverse-proxy to the backend catalog.
//
// Flags:
//
//	-listen         address to bind (default 127.0.0.1:8182)
//	-backend        catalog to proxy to (default http://127.0.0.1:8181)
//	-client-id      accepted client_id (default icegres)
//	-client-secret  accepted client_secret (default supersecret)
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
)

// tokenStore holds the opaque bearer tokens this gateway has minted. Any token
// it minted (via the client-credentials grant OR handed to a client as a
// pre-minted `token`) is valid until the process exits; expiry is advisory.
type tokenStore struct {
	mu     sync.RWMutex
	tokens map[string]struct{}
}

func newTokenStore() *tokenStore {
	return &tokenStore{tokens: make(map[string]struct{})}
}

func (s *tokenStore) mint() string {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is unrecoverable for an auth gateway.
		log.Fatalf("catalog-gateway: crypto/rand failed: %v", err)
	}
	tok := hex.EncodeToString(buf)
	s.mu.Lock()
	s.tokens[tok] = struct{}{}
	s.mu.Unlock()
	return tok
}

func (s *tokenStore) valid(tok string) bool {
	s.mu.RLock()
	_, ok := s.tokens[tok]
	s.mu.RUnlock()
	return ok
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8182", "address to bind the gateway on")
	backend := flag.String("backend", "http://127.0.0.1:8181", "backend Iceberg REST catalog to proxy to")
	clientID := flag.String("client-id", "icegres", "accepted OAuth2 client_id")
	clientSecret := flag.String("client-secret", "supersecret", "accepted OAuth2 client_secret")
	preMinted := flag.String("pre-mint", "", "optional: also accept this fixed pre-minted bearer token (for the `token` prop smoke)")
	flag.Parse()

	backendURL, err := url.Parse(*backend)
	if err != nil {
		log.Fatalf("catalog-gateway: bad -backend %q: %v", *backend, err)
	}

	store := newTokenStore()
	if *preMinted != "" {
		store.mu.Lock()
		store.tokens[*preMinted] = struct{}{}
		store.mu.Unlock()
	}

	proxy := httputil.NewSingleHostReverseProxy(backendURL)
	// Preserve the exact incoming path (NewSingleHostReverseProxy already
	// forwards it) and set the Host header to the backend so Lakekeeper's
	// routing matches.
	origDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		origDirector(r)
		r.Host = backendURL.Host
	}

	mux := http.NewServeMux()

	// OPEN: the OAuth2 token endpoint (client-credentials grant).
	mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		if r.PostForm.Get("grant_type") != "client_credentials" {
			writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type")
			return
		}
		if r.PostForm.Get("client_id") != *clientID ||
			r.PostForm.Get("client_secret") != *clientSecret {
			// Do not echo the offered credentials back.
			log.Printf("token request REJECTED (bad client credentials, scope=%q)", r.PostForm.Get("scope"))
			writeOAuthError(w, http.StatusUnauthorized, "invalid_client")
			return
		}
		tok := store.mint()
		log.Printf("token request OK (client_id=%s scope=%q) -> minted bearer", *clientID, r.PostForm.Get("scope"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": tok,
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	// Everything else: bearer-gated proxy (with an OPEN config exception).
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if isOpenDiscovery(r) {
			proxy.ServeHTTP(w, r)
			return
		}
		tok, ok := bearerToken(r)
		if !ok || !store.valid(tok) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="icegres-catalog", error="invalid_token"`)
			log.Printf("401 %s %s (no valid bearer)", r.Method, r.URL.Path)
			writeOAuthError(w, http.StatusUnauthorized, "invalid_token")
			return
		}
		proxy.ServeHTTP(w, r)
	})

	log.Printf("catalog-gateway listening on %s -> %s (client_id=%s)", *listen, *backend, *clientID)
	srv := &http.Server{Addr: *listen, Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("catalog-gateway: %v", err)
	}
}

// isOpenDiscovery reports whether the request is the unauthenticated Iceberg
// REST config handshake (GET .../v1/config), which advertises the token
// endpoint and must be reachable before a client holds a bearer.
func isOpenDiscovery(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.HasSuffix(pathOnly(r.URL.Path), "/v1/config")
}

func pathOnly(p string) string {
	if i := strings.IndexByte(p, '?'); i >= 0 {
		return p[:i]
	}
	return p
}

// bearerToken extracts the token from an `Authorization: Bearer <t>` header.
func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	tok := strings.TrimSpace(h[len(prefix):])
	if tok == "" {
		return "", false
	}
	return tok, true
}

func writeOAuthError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}
