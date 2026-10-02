package core

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// GPU is a GPU model applicants may declare.
type GPU struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	VRAMMB    int       `json:"vram_mb"`
	Accepted  bool      `json:"accepted"`
	CreatedAt time.Time `json:"created_at"`
}

// GPUInput is what the operator supplies for a GPU model.
type GPUInput struct {
	Name     string `json:"name"`
	VRAMMB   int    `json:"vram_mb"`
	Accepted *bool  `json:"accepted,omitempty"`
}

func (in GPUInput) validate() (GPUInput, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" || len(in.Name) > 80 {
		return in, invalidf("GPU name must be 1 to 80 characters")
	}
	if in.VRAMMB <= 0 {
		return in, invalidf("GPU memory must be positive")
	}
	return in, nil
}

// GPUs lists the supported GPU models; onlyAccepted leaves out retired ones.
func (s *Service) GPUs(ctx context.Context, onlyAccepted bool) ([]GPU, error) {
	query := `SELECT id, name, vram_mb, accepted, created_at FROM gpus`
	if onlyAccepted {
		query += ` WHERE accepted = 1`
	}
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	gpus := []GPU{}
	for rows.Next() {
		var g GPU
		var created int64
		if err := rows.Scan(&g.ID, &g.Name, &g.VRAMMB, &g.Accepted, &created); err != nil {
			return nil, err
		}
		g.CreatedAt = fromMs(created)
		gpus = append(gpus, g)
	}
	return gpus, rows.Err()
}

func (s *Service) gpu(ctx context.Context, id string) (*GPU, error) {
	var g GPU
	var created int64
	err := s.db.QueryRowContext(ctx, `SELECT id, name, vram_mb, accepted, created_at FROM gpus WHERE id = ?`, id).
		Scan(&g.ID, &g.Name, &g.VRAMMB, &g.Accepted, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFoundf("GPU %s is not in the supported list", id)
	}
	if err != nil {
		return nil, err
	}
	g.CreatedAt = fromMs(created)
	return &g, nil
}

// CreateGPU adds a GPU model to the supported list.
func (s *Service) CreateGPU(ctx context.Context, actor string, in GPUInput) (*GPU, error) {
	in, err := in.validate()
	if err != nil {
		return nil, err
	}
	accepted := in.Accepted == nil || *in.Accepted
	id := newID("gpu")
	if _, err := s.db.ExecContext(ctx, `INSERT INTO gpus (id, name, vram_mb, accepted, created_at) VALUES (?, ?, ?, ?, ?)`,
		id, in.Name, in.VRAMMB, accepted, s.nowMs()); err != nil {
		if isUniqueViolation(err) {
			return nil, conflictf("a GPU named %q is already listed", in.Name)
		}
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "gpu.create", id, in.Name); err != nil {
		return nil, err
	}
	return s.gpu(ctx, id)
}

// UpdateGPU changes a GPU model. Retiring one (accepted false) stops new
// applications from choosing it and leaves existing machines alone.
func (s *Service) UpdateGPU(ctx context.Context, actor, id string, in GPUInput) (*GPU, error) {
	current, err := s.gpu(ctx, id)
	if err != nil {
		return nil, err
	}
	in, err = in.validate()
	if err != nil {
		return nil, err
	}
	accepted := current.Accepted
	if in.Accepted != nil {
		accepted = *in.Accepted
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE gpus SET name = ?, vram_mb = ?, accepted = ? WHERE id = ?`, in.Name, in.VRAMMB, accepted, id); err != nil {
		if isUniqueViolation(err) {
			return nil, conflictf("a GPU named %q is already listed", in.Name)
		}
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "gpu.update", id, mustJSON(in)); err != nil {
		return nil, err
	}
	return s.gpu(ctx, id)
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
