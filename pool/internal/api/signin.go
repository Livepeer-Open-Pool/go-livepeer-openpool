package api

import (
	"net/http"
	"time"
)

func (s *Server) walletNonce(w http.ResponseWriter, r *http.Request) {
	nonce, err := s.wallet.Nonce(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nonce": nonce, "sign_in": s.signIn})
}

type sessionBody struct {
	Token     string    `json:"token"`
	Role      string    `json:"role"`
	Subject   string    `json:"subject"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Server) walletVerify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Message   string `json:"message"`
		Signature string `json:"signature"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	token, id, err := s.wallet.SignIn(r.Context(), req.Message, req.Signature)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, sessionBody{Token: token, Role: id.Role, Subject: id.Subject, ExpiresAt: id.ExpiresAt})
}

func (s *Server) adminCode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string `json:"email"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.admin.RequestCode(r.Context(), req.Email); err != nil {
		s.writeError(w, r, err)
		return
	}
	// The same answer whether or not the address is the admin's.
	noContent(w)
}

func (s *Server) adminVerify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string `json:"email"`
		Code  string `json:"code"`
	}
	if err := readJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	token, id, err := s.admin.SignIn(r.Context(), req.Email, req.Code)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, sessionBody{Token: token, Role: id.Role, Subject: id.Subject, ExpiresAt: id.ExpiresAt})
}
