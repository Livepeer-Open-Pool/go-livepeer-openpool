package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Application statuses.
const (
	ApplicationPending  = "pending"
	ApplicationApproved = "approved"
	ApplicationRejected = "rejected"
)

// MachineGPU is some number of one GPU model in a machine.
type MachineGPU struct {
	GPUID string `json:"gpu_id"`
	Count int    `json:"count"`
}

// MachineSpec is a machine as a person declares it.
type MachineSpec struct {
	Label string       `json:"label"`
	GPUs  []MachineGPU `json:"gpus"`
}

// ApplicationDraft is what an applicant fills in.
type ApplicationDraft struct {
	DisplayName string        `json:"display_name"`
	Machines    []MachineSpec `json:"machines"`
}

// Application is a request to join the pool in this region.
type Application struct {
	ID                   string        `json:"id"`
	Address              string        `json:"address"`
	DisplayName          string        `json:"display_name"`
	Machines             []MachineSpec `json:"machines"`
	GlobalTermsVersion   int           `json:"global_terms_version"`
	GlobalTermsHash      string        `json:"global_terms_hash"`
	RegionalTermsVersion int           `json:"regional_terms_version,omitempty"`
	RegionalTermsHash    string        `json:"regional_terms_hash,omitempty"`
	Status               string        `json:"status"`
	Reason               string        `json:"reason,omitempty"`
	CreatedAt            time.Time     `json:"created_at"`
	DecidedAt            *time.Time    `json:"decided_at,omitempty"`
}

func validName(name string, max int) bool {
	return name != "" && len(name) <= max && strings.IndexFunc(name, func(r rune) bool { return unicode.IsControl(r) }) < 0
}

func (s *Service) validateMachineSpec(ctx context.Context, spec MachineSpec, requireAccepted bool) (MachineSpec, error) {
	spec.Label = strings.TrimSpace(spec.Label)
	if !validName(spec.Label, 40) {
		return spec, invalidf("a machine's label must be 1 to 40 printable characters")
	}
	if len(spec.GPUs) == 0 || len(spec.GPUs) > 8 {
		return spec, invalidf("machine %q must list between 1 and 8 GPU models", spec.Label)
	}
	seen := map[string]bool{}
	for _, g := range spec.GPUs {
		if g.Count < 1 || g.Count > 16 {
			return spec, invalidf("machine %q: a GPU count must be between 1 and 16", spec.Label)
		}
		if seen[g.GPUID] {
			return spec, invalidf("machine %q lists the same GPU model twice", spec.Label)
		}
		seen[g.GPUID] = true
		gpu, err := s.gpu(ctx, g.GPUID)
		if err != nil {
			return spec, invalidf("machine %q: GPU %s is not in the supported list", spec.Label, g.GPUID)
		}
		if requireAccepted && !gpu.Accepted {
			return spec, invalidf("machine %q: %s is no longer accepted", spec.Label, gpu.Name)
		}
	}
	return spec, nil
}

func (s *Service) validateDraft(ctx context.Context, draft ApplicationDraft) (ApplicationDraft, error) {
	draft.DisplayName = strings.TrimSpace(draft.DisplayName)
	if !validName(draft.DisplayName, 40) {
		return draft, invalidf("display name must be 1 to 40 printable characters")
	}
	if len(draft.Machines) == 0 || len(draft.Machines) > 20 {
		return draft, invalidf("an application lists between 1 and 20 machines")
	}
	labels := map[string]bool{}
	for i, spec := range draft.Machines {
		spec, err := s.validateMachineSpec(ctx, spec, true)
		if err != nil {
			return draft, err
		}
		if labels[spec.Label] {
			return draft, invalidf("two machines are both labelled %q", spec.Label)
		}
		labels[spec.Label] = true
		draft.Machines[i] = spec
	}
	return draft, nil
}

// applicationMessage is the text an applicant signs. It names what they are
// accepting, so the signature is their acceptance of those terms.
func (s *Service) applicationMessage(address string, draft ApplicationDraft, terms CurrentTerms) string {
	var b strings.Builder
	b.WriteString("Open Pool application\n\n")
	fmt.Fprintf(&b, "Region: %s\n", s.cfg.RegionID)
	fmt.Fprintf(&b, "Address: %s\n", address)
	fmt.Fprintf(&b, "Display name: %s\n", draft.DisplayName)
	fmt.Fprintf(&b, "Machines: %d\n", len(draft.Machines))
	fmt.Fprintf(&b, "Global terms: version %d, sha256 %s\n", terms.Global.Version, terms.Global.Hash)
	if terms.Regional != nil {
		fmt.Fprintf(&b, "Regional terms: version %d, sha256 %s\n", terms.Regional.Version, terms.Regional.Hash)
	} else {
		b.WriteString("Regional terms: none\n")
	}
	b.WriteString("\nBy signing I accept these terms. Payouts go to the address above, which cannot be changed.")
	return b.String()
}

// ApplicationMessage returns the exact text the applicant must sign to submit draft.
func (s *Service) ApplicationMessage(ctx context.Context, address string, draft ApplicationDraft) (string, error) {
	address, draft, terms, err := s.checkApplication(ctx, address, draft)
	if err != nil {
		return "", err
	}
	return s.applicationMessage(address, draft, terms), nil
}

// checkApplication validates a draft and that address may apply now.
func (s *Service) checkApplication(ctx context.Context, address string, draft ApplicationDraft) (string, ApplicationDraft, CurrentTerms, error) {
	address, ok := normalizeAddress(address)
	if !ok {
		return "", draft, CurrentTerms{}, invalidf("address is not an Ethereum address")
	}
	draft, err := s.validateDraft(ctx, draft)
	if err != nil {
		return "", draft, CurrentTerms{}, err
	}
	terms, err := s.Terms(ctx)
	if err != nil {
		return "", draft, CurrentTerms{}, err
	}
	if terms.Global == nil {
		return "", draft, CurrentTerms{}, conflictf("the pool has not published its terms yet, so it is not taking applications")
	}
	member, err := s.memberByAddress(ctx, address)
	if err != nil {
		return "", draft, CurrentTerms{}, err
	}
	if member != nil {
		switch member.Status {
		case MemberActive:
			return "", draft, CurrentTerms{}, conflictf("this address is already a member here; add machines from your machines page")
		case MemberBanned:
			return "", draft, CurrentTerms{}, forbiddenf("this address is banned from the pool")
		}
	}
	var pending int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM applications WHERE address = ? AND status = ?`, address, ApplicationPending).Scan(&pending); err != nil {
		return "", draft, CurrentTerms{}, err
	}
	if pending > 0 {
		return "", draft, CurrentTerms{}, conflictf("this address already has an application waiting for a decision")
	}
	return address, draft, terms, nil
}

// SubmitApplication records an application signed by address. The signature
// must be over ApplicationMessage for the same draft and the terms in force.
func (s *Service) SubmitApplication(ctx context.Context, address string, draft ApplicationDraft, signature string) (*Application, error) {
	address, draft, terms, err := s.checkApplication(ctx, address, draft)
	if err != nil {
		return nil, err
	}
	message := s.applicationMessage(address, draft, terms)
	signer, err := s.cfg.RecoverSigner(message, signature)
	if err != nil {
		return nil, invalidf("the signature is not valid: %v", err)
	}
	if !strings.EqualFold(signer, address) {
		// A draft or terms that changed after the message was fetched also
		// lands here: the signature is then over a different text.
		return nil, conflictf("the signature does not match this application; fetch the message again and sign it")
	}
	app := Application{
		ID: newID("apl"), Address: address, DisplayName: draft.DisplayName, Machines: draft.Machines,
		GlobalTermsVersion: terms.Global.Version, GlobalTermsHash: terms.Global.Hash, Status: ApplicationPending,
	}
	if terms.Regional != nil {
		app.RegionalTermsVersion, app.RegionalTermsHash = terms.Regional.Version, terms.Regional.Hash
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO applications (id, address, display_name, machines, global_terms_version, global_terms_hash,
		regional_terms_version, regional_terms_hash, message, signature, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		app.ID, app.Address, app.DisplayName, mustJSON(app.Machines), app.GlobalTermsVersion, app.GlobalTermsHash,
		app.RegionalTermsVersion, app.RegionalTermsHash, message, signature, app.Status, s.nowMs())
	if err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, address, "application.submit", app.ID, app.DisplayName); err != nil {
		return nil, err
	}
	s.notifyAdmin(ctx, fmt.Sprintf("Open Pool %s: application from %s needs approval", s.cfg.RegionName, app.DisplayName),
		fmt.Sprintf("%s (%s) applied to join %s with %d machine(s).\n\nReview it: %s",
			app.DisplayName, app.Address, s.cfg.RegionName, len(app.Machines), s.adminLink("applications/"+app.ID)))
	return s.Application(ctx, app.ID)
}

func (s *Service) adminLink(path string) string {
	return strings.TrimRight(s.cfg.AdminURL, "/") + "/" + path + "?region=" + s.cfg.RegionID
}

const applicationColumns = `id, address, display_name, machines, global_terms_version, global_terms_hash,
	regional_terms_version, regional_terms_hash, status, reason, created_at, decided_at`

func scanApplication(row rowScanner) (*Application, error) {
	var a Application
	var machines string
	var created, decided int64
	if err := row.Scan(&a.ID, &a.Address, &a.DisplayName, &machines, &a.GlobalTermsVersion, &a.GlobalTermsHash,
		&a.RegionalTermsVersion, &a.RegionalTermsHash, &a.Status, &a.Reason, &created, &decided); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(machines), &a.Machines); err != nil {
		return nil, err
	}
	a.CreatedAt, a.DecidedAt = fromMs(created), optionalTime(decided)
	return &a, nil
}

// Application returns one application.
func (s *Service) Application(ctx context.Context, id string) (*Application, error) {
	a, err := scanApplication(s.db.QueryRowContext(ctx, `SELECT `+applicationColumns+` FROM applications WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFoundf("application %s does not exist", id)
	}
	return a, err
}

// Applications lists applications, newest first. status and address filter
// when not empty.
func (s *Service) Applications(ctx context.Context, status, address string) ([]Application, error) {
	query, args := `SELECT `+applicationColumns+` FROM applications WHERE 1 = 1`, []any{}
	if status != "" {
		query, args = query+` AND status = ?`, append(args, status)
	}
	if address != "" {
		normalized, ok := normalizeAddress(address)
		if !ok {
			return []Application{}, nil
		}
		query, args = query+` AND address = ?`, append(args, normalized)
	}
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY created_at DESC, id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	apps := []Application{}
	for rows.Next() {
		a, err := scanApplication(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *a)
	}
	return apps, rows.Err()
}

// MachineAssignments are the apps the operator assigns to one of an
// application's machines when approving it, by the machine's position in
// the application.
type MachineAssignments struct {
	Machine     int               `json:"machine"`
	Assignments []AssignmentInput `json:"assignments"`
}

// ApproveApplication makes the applicant a member with the machines they
// declared, each already approved, and assigns the apps given. Machines
// without assignments stay idle until the operator assigns them.
func (s *Service) ApproveApplication(ctx context.Context, actor, id string, assignments []MachineAssignments) (*Member, error) {
	app, err := s.Application(ctx, id)
	if err != nil {
		return nil, err
	}
	if app.Status != ApplicationPending {
		return nil, conflictf("application %s was already %s", id, app.Status)
	}
	byMachine := map[int][]AssignmentInput{}
	for _, ma := range assignments {
		if ma.Machine < 0 || ma.Machine >= len(app.Machines) {
			return nil, invalidf("the application has no machine %d", ma.Machine)
		}
		byMachine[ma.Machine] = ma.Assignments
	}
	// Validate every assignment before anything is written.
	resolved := map[int][]resolvedAssignment{}
	for index, inputs := range byMachine {
		r, err := s.resolveAssignments(ctx, app.Machines[index], inputs)
		if err != nil {
			return nil, err
		}
		resolved[index] = r
	}

	existing, err := s.memberByAddress(ctx, app.Address)
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.Status != MemberLeft {
		return nil, conflictf("this address is already a %s member", existing.Status)
	}

	now := s.nowMs()
	memberID := newID("mbr")
	err = s.db.Tx(ctx, func(tx *sql.Tx) error {
		if existing != nil {
			memberID = existing.ID
			if _, err := tx.ExecContext(ctx, `UPDATE members SET status = ?, display_name = ?, reason = '', updated_at = ? WHERE id = ?`,
				MemberActive, app.DisplayName, now, memberID); err != nil {
				return err
			}
		} else if _, err := tx.ExecContext(ctx, `INSERT INTO members (id, address, display_name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
			memberID, app.Address, app.DisplayName, MemberActive, now, now); err != nil {
			return err
		}
		for index, spec := range app.Machines {
			machineID := newID("mch")
			if _, err := tx.ExecContext(ctx, `INSERT INTO machines (id, member_id, label, gpus, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
				machineID, memberID, spec.Label, mustJSON(spec.GPUs), MachineApproved, now); err != nil {
				return err
			}
			for _, a := range resolved[index] {
				if _, err := tx.ExecContext(ctx, insertAssignment,
					newID("asg"), machineID, a.app.ID, a.capacity, AssignmentActive, a.app.RequiredVersion, mustJSON(a.app.Container), now, now); err != nil {
					return err
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE applications SET status = ?, decided_at = ? WHERE id = ? AND status = ?`,
			ApplicationApproved, now, id, ApplicationPending); err != nil {
			return err
		}
		return s.audit(ctx, tx, actor, "application.approve", id, memberID)
	})
	if err != nil {
		return nil, err
	}
	return s.Member(ctx, memberID)
}

// RejectApplication turns an application down; the applicant sees the reason.
func (s *Service) RejectApplication(ctx context.Context, actor, id, reason string) (*Application, error) {
	app, err := s.Application(ctx, id)
	if err != nil {
		return nil, err
	}
	if app.Status != ApplicationPending {
		return nil, conflictf("application %s was already %s", id, app.Status)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, invalidf("a rejection needs a reason, which the applicant sees")
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE applications SET status = ?, reason = ?, decided_at = ? WHERE id = ?`,
		ApplicationRejected, reason, s.nowMs(), id); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "application.reject", id, reason); err != nil {
		return nil, err
	}
	return s.Application(ctx, id)
}
