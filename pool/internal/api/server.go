// Package api is the pool service's HTTP API: what the sites, the member
// agent and the orchestrator call. It decodes, authenticates and encodes;
// the rules live in core.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Livepeer-Open-Pool/pool/internal/auth"
	"github.com/Livepeer-Open-Pool/pool/internal/core"
	"github.com/Livepeer-Open-Pool/pool/internal/ledger"
	"github.com/Livepeer-Open-Pool/pool/internal/payouts"
)

// Server serves the API.
type Server struct {
	core       *core.Service
	ledger     *ledger.Ledger
	payouts    *payouts.Service
	now        func() time.Time
	tokens     *auth.Tokens
	wallet     *auth.Wallet
	admin      *auth.Admin
	poolSecret string
	origins    map[string]bool
	signIn     SignInInfo
	agent      *AgentOptions
	log        *slog.Logger
}

// SignInInfo is what a site needs to build a wallet sign-in message this
// service will accept.
type SignInInfo struct {
	Domains []string `json:"domains"`
	ChainID int      `json:"chain_id"`
}

// Options configure a Server.
type Options struct {
	Core *core.Service
	// Ledger and Payouts are the pool's books and its payments.
	Ledger  *ledger.Ledger
	Payouts *payouts.Service
	// Now is the clock; nil means time.Now.
	Now    func() time.Time
	Tokens *auth.Tokens
	Wallet *auth.Wallet
	Admin  *auth.Admin
	// PoolSecret is what the orchestrator presents on the admission webhook.
	PoolSecret string
	// AllowedOrigins are the sites whose pages may call this API from a browser.
	AllowedOrigins []string
	SignIn         SignInInfo
	// Agent is nil for a pool whose machines are not run by the agent.
	Agent  *AgentOptions
	Logger *slog.Logger
}

// New returns a Server.
func New(opts Options) *Server {
	origins := map[string]bool{}
	for _, o := range opts.AllowedOrigins {
		origins[strings.TrimRight(o, "/")] = true
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Server{
		core: opts.Core, ledger: opts.Ledger, payouts: opts.Payouts, now: now,
		tokens: opts.Tokens, wallet: opts.Wallet, admin: opts.Admin,
		poolSecret: opts.PoolSecret, origins: origins, signIn: opts.SignIn, agent: opts.Agent, log: log,
	}
}

// Handler returns the API's routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/orchestrator/admit", s.admit)
	mux.HandleFunc("POST /v1/orchestrator/events", s.orchestratorEvents)
	mux.HandleFunc("GET /v1/agent/desired", s.agentDesired)
	mux.HandleFunc("POST /v1/agent/report", s.agentReport)

	mux.HandleFunc("GET /v1/public/pool", s.publicPool)
	mux.HandleFunc("GET /v1/public/apps", s.publicApps)
	mux.HandleFunc("GET /v1/public/gpus", s.publicGPUs)
	mux.HandleFunc("GET /v1/public/terms", s.publicTerms)
	mux.HandleFunc("GET /v1/public/leaderboard", s.publicLeaderboard)

	mux.HandleFunc("POST /v1/auth/wallet/nonce", s.walletNonce)
	mux.HandleFunc("POST /v1/auth/wallet/verify", s.walletVerify)
	mux.HandleFunc("POST /v1/auth/admin/code", s.adminCode)
	mux.HandleFunc("POST /v1/auth/admin/verify", s.adminVerify)

	mux.HandleFunc("POST /v1/applications/message", s.wallet_(s.applicationMessage))
	mux.HandleFunc("POST /v1/applications", s.wallet_(s.submitApplication))
	mux.HandleFunc("GET /v1/me", s.wallet_(s.me))
	mux.HandleFunc("GET /v1/me/machines", s.member_(s.myMachines))
	mux.HandleFunc("POST /v1/me/machines", s.member_(s.requestMachine))
	mux.HandleFunc("DELETE /v1/me/machines/{id}", s.member_(s.removeMyMachine))
	mux.HandleFunc("POST /v1/me/machines/{id}/credential", s.member_(s.createCredential))
	mux.HandleFunc("DELETE /v1/me/machines/{id}/credential", s.member_(s.revokeCredential))
	mux.HandleFunc("GET /v1/me/runners", s.member_(s.myRunners))
	mux.HandleFunc("GET /v1/me/tests", s.member_(s.myTests))
	mux.HandleFunc("GET /v1/me/earnings", s.member_(s.myEarnings))
	mux.HandleFunc("GET /v1/me/payouts", s.member_(s.myPayouts))

	mux.HandleFunc("GET /v1/admin/status", s.admin_(s.adminStatus))
	mux.HandleFunc("GET /v1/admin/applications", s.admin_(s.adminApplications))
	mux.HandleFunc("GET /v1/admin/applications/{id}", s.admin_(s.adminApplication))
	mux.HandleFunc("POST /v1/admin/applications/{id}/approve", s.admin_(s.approveApplication))
	mux.HandleFunc("POST /v1/admin/applications/{id}/reject", s.admin_(s.rejectApplication))
	mux.HandleFunc("GET /v1/admin/members", s.admin_(s.adminMembers))
	mux.HandleFunc("GET /v1/admin/members/{id}", s.admin_(s.adminMember))
	mux.HandleFunc("POST /v1/admin/members/{id}/ban", s.admin_(s.banMember))
	mux.HandleFunc("POST /v1/admin/members/{id}/unban", s.admin_(s.unbanMember))
	mux.HandleFunc("GET /v1/admin/machines", s.admin_(s.adminMachines))
	mux.HandleFunc("POST /v1/admin/machines/{id}/approve", s.admin_(s.approveMachine))
	mux.HandleFunc("PUT /v1/admin/machines/{id}/assignments", s.admin_(s.setAssignments))
	mux.HandleFunc("DELETE /v1/admin/machines/{id}", s.admin_(s.adminRemoveMachine))
	mux.HandleFunc("GET /v1/admin/gpus", s.admin_(s.adminGPUs))
	mux.HandleFunc("POST /v1/admin/gpus", s.admin_(s.createGPU))
	mux.HandleFunc("PUT /v1/admin/gpus/{id}", s.admin_(s.updateGPU))
	mux.HandleFunc("GET /v1/admin/apps", s.admin_(s.adminApps))
	mux.HandleFunc("POST /v1/admin/apps", s.admin_(s.createApp))
	mux.HandleFunc("GET /v1/admin/apps/{id}", s.admin_(s.adminApp))
	mux.HandleFunc("PUT /v1/admin/apps/{id}", s.admin_(s.updateApp))
	mux.HandleFunc("POST /v1/admin/apps/{id}/version", s.admin_(s.setAppVersion))
	mux.HandleFunc("POST /v1/admin/apps/{id}/pause", s.admin_(s.pauseApp))
	mux.HandleFunc("POST /v1/admin/apps/{id}/resume", s.admin_(s.resumeApp))
	mux.HandleFunc("GET /v1/admin/terms", s.admin_(s.adminTerms))
	mux.HandleFunc("POST /v1/admin/terms", s.admin_(s.publishTerms))
	mux.HandleFunc("GET /v1/admin/settings", s.admin_(s.getSettings))
	mux.HandleFunc("PUT /v1/admin/settings", s.admin_(s.putSettings))
	mux.HandleFunc("GET /v1/admin/runners", s.admin_(s.adminRunners))
	mux.HandleFunc("POST /v1/admin/runners/{id}/state", s.admin_(s.setRunnerState))
	mux.HandleFunc("POST /v1/admin/runners/{id}/evict", s.admin_(s.evictRunner))
	mux.HandleFunc("POST /v1/admin/runners/{id}/hold", s.admin_(s.holdRunner))
	mux.HandleFunc("POST /v1/admin/runners/{id}/test", s.admin_(s.testRunner))
	mux.HandleFunc("GET /v1/admin/tests", s.admin_(s.adminTests))
	mux.HandleFunc("GET /v1/admin/audit", s.admin_(s.adminAudit))
	mux.HandleFunc("GET /v1/admin/demand", s.admin_(s.adminDemand))
	mux.HandleFunc("GET /v1/admin/batches", s.admin_(s.adminBatches))
	mux.HandleFunc("GET /v1/admin/batches/{id}", s.admin_(s.adminBatch))
	mux.HandleFunc("PUT /v1/admin/batches/{id}/items/{member}", s.admin_(s.setBatchItem))
	mux.HandleFunc("POST /v1/admin/batches/{id}/approve", s.admin_(s.approveBatch))
	mux.HandleFunc("POST /v1/admin/batches/{id}/retry", s.admin_(s.retryBatch))
	mux.HandleFunc("GET /v1/admin/wallet", s.admin_(s.adminWallet))

	return s.cors(mux)
}

// cors lets pages on the allowed sites call the API from a browser. Tokens
// travel in the Authorization header, never in cookies, so no credentials
// mode is needed.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && s.origins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
				w.Header().Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorBody struct {
	Error string `json:"error"`
}

// writeError answers with the status the error's kind maps to. An error
// that is not one of ours is a fault here: it is logged and the caller is
// told only that something went wrong.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var coreErr *core.Error
	switch {
	case errors.As(err, &coreErr):
		status := http.StatusBadRequest
		switch coreErr.Kind {
		case core.NotFound:
			status = http.StatusNotFound
		case core.Conflict:
			status = http.StatusConflict
		case core.Forbidden:
			status = http.StatusForbidden
		case core.Unavailable:
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, errorBody{coreErr.Message})
	case errors.Is(err, auth.ErrInvalid):
		writeJSON(w, http.StatusUnauthorized, errorBody{"not signed in"})
	case errors.Is(err, auth.ErrTooSoon):
		writeJSON(w, http.StatusTooManyRequests, errorBody{err.Error()})
	case errors.Is(err, errBadRequest):
		writeJSON(w, http.StatusBadRequest, errorBody{err.Error()})
	default:
		s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeJSON(w, http.StatusInternalServerError, errorBody{"something went wrong on our side"})
	}
}

var errBadRequest = errors.New("the request body is not the JSON this endpoint takes")

// readJSON decodes a JSON body of bounded size. An empty body leaves v as it is.
func readJSON(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return errBadRequest
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, v); err != nil {
		return errBadRequest
	}
	return nil
}

func bearer(r *http.Request) string {
	value := r.Header.Get("Authorization")
	if token, ok := strings.CutPrefix(value, "Bearer "); ok {
		return strings.TrimSpace(token)
	}
	return ""
}

func (s *Server) identity(r *http.Request, role string) (*auth.Identity, error) {
	id, err := s.tokens.Verify(bearer(r))
	if err != nil || id.Role != role {
		return nil, auth.ErrInvalid
	}
	return id, nil
}

type walletHandler func(w http.ResponseWriter, r *http.Request, address string)

// wallet_ requires a wallet session: an applicant or a member.
func (s *Server) wallet_(next walletHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := s.identity(r, auth.RoleWallet)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		next(w, r, id.Subject)
	}
}

type memberHandler func(w http.ResponseWriter, r *http.Request, member *core.Member)

// member_ requires a wallet session whose address is a member in this region.
func (s *Server) member_(next memberHandler) http.HandlerFunc {
	return s.wallet_(func(w http.ResponseWriter, r *http.Request, address string) {
		member, err := s.core.MemberByAddress(r.Context(), address)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		if member == nil {
			s.writeError(w, r, &core.Error{Kind: core.Forbidden, Message: "this address is not a member in this region"})
			return
		}
		next(w, r, member)
	})
}

type adminHandler func(w http.ResponseWriter, r *http.Request, actor string)

// admin_ requires an operator session.
func (s *Server) admin_(next adminHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := s.identity(r, auth.RoleAdmin)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		next(w, r, "admin:"+id.Subject)
	}
}

// admit answers the orchestrator's admission webhook. Only the orchestrator
// holding the pool secret is answered: the request carries member credentials.
func (s *Server) admit(w http.ResponseWriter, r *http.Request) {
	if !s.poolSecretOK(r) {
		writeJSON(w, http.StatusUnauthorized, errorBody{"the pool secret is missing or wrong"})
		return
	}
	var req core.AdmissionRequest
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	resp, err := s.core.Admit(r.Context(), req)
	if err != nil {
		// Anything but a 200 makes the orchestrator refuse the registration,
		// which is right when no decision could be reached.
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// poolSecretOK says whether the caller presented the pool secret, which
// only the orchestrator and the operator hold.
func (s *Server) poolSecretOK(r *http.Request) bool {
	return s.poolSecret != "" && subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(s.poolSecret)) == 1
}

func noContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }
