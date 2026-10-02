package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// Runner modes, as the orchestrator names them.
const (
	ModePersistent = "persistent"
	ModeSingleShot = "single-shot"
)

// Price is what the orchestrator charges for an app, in US dollars.
type Price struct {
	Amount string `json:"price"`
	// Unit is "hour", "720p" (an hour of 720p video) or "fixed" (per request).
	Unit string `json:"unit"`
}

// Container says how a member's machine runs an app.
type Container struct {
	// Image is pinned by digest: name@sha256:….
	Image   string            `json:"image"`
	Command []string          `json:"command,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	// Port is where the app listens inside its container.
	Port int `json:"port"`
	// HealthPath answers 200 while the app can take work; empty means it is
	// not checked.
	HealthPath string `json:"health_path"`
	// GPUs is how many of the machine's GPUs the app gets; 0 means all.
	GPUs int `json:"gpus"`
	// NoGPU is for an app that needs no GPU at all.
	NoGPU bool `json:"no_gpu,omitempty"`
	// ShmSize is the container's shared memory, e.g. "8g"; empty leaves Docker's default.
	ShmSize string `json:"shm_size,omitempty"`
	// CachePath is where the app keeps model files; a volume is mounted there
	// so they survive restarts. Empty mounts nothing.
	CachePath string `json:"cache_path,omitempty"`
}

// Test is the operator's check that a runner really serves its app.
type Test struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	// ExpectStatus is the status a good answer has.
	ExpectStatus int `json:"expect_status"`
	// ExpectContains are strings a good answer's body contains.
	ExpectContains []string `json:"expect_contains,omitempty"`
	// TimeoutMs is how long a good answer may take.
	TimeoutMs int `json:"timeout_ms"`
}

// Defined says whether the operator wrote a test for the app.
func (t Test) Defined() bool { return t.Path != "" }

// App is something customers can ask the pool for and members' machines run.
type App struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Mode            string `json:"mode"`
	Price           Price  `json:"price"`
	RequiredVersion string `json:"required_version"`
	// PreviousVersion is still admitted until PreviousVersionDeadline.
	PreviousVersion         string     `json:"previous_version,omitempty"`
	PreviousVersionDeadline *time.Time `json:"previous_version_deadline,omitempty"`
	Container               Container  `json:"container"`
	// EligibleGPUs are the GPU models a machine needs one of to be assigned
	// the app; empty means any.
	EligibleGPUs    []string  `json:"eligible_gpus"`
	DefaultCapacity int       `json:"default_capacity"`
	MaxCapacity     int       `json:"max_capacity"`
	Test            Test      `json:"test"`
	Discovery       string    `json:"discovery"`
	Description     string    `json:"description"`
	Offered         bool      `json:"offered"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// AppInput is what the operator supplies for an app. Name, RequiredVersion
// and Container are set at creation. The version and the container change
// together through SetAppVersion, which rolls the change out to machines,
// and the name never changes, since runners and prices are known by it.
type AppInput struct {
	Name            string    `json:"name"`
	Mode            string    `json:"mode"`
	Price           Price     `json:"price"`
	RequiredVersion string    `json:"required_version"`
	Container       Container `json:"container"`
	EligibleGPUs    []string  `json:"eligible_gpus"`
	DefaultCapacity int       `json:"default_capacity"`
	MaxCapacity     int       `json:"max_capacity"`
	Test            Test      `json:"test"`
	Discovery       string    `json:"discovery"`
	Description     string    `json:"description"`
}

var pinnedImagePattern = regexp.MustCompile(`^[^\s@]+@sha256:[0-9a-f]{64}$`)

func validatePrice(p Price) (Price, error) {
	p.Amount = strings.TrimSpace(p.Amount)
	amount, ok := new(big.Rat).SetString(p.Amount)
	if !ok || amount.Sign() <= 0 {
		return p, invalidf("price must be a positive number")
	}
	switch p.Unit {
	case "hour", "720p", "fixed":
	default:
		return p, invalidf("price unit must be hour, 720p, or fixed")
	}
	return p, nil
}

func validateContainer(c Container) (Container, error) {
	c.Image = strings.TrimSpace(c.Image)
	if !pinnedImagePattern.MatchString(c.Image) {
		return c, invalidf("container image must be pinned by digest, as name@sha256:<64 hex digits>")
	}
	if c.Port < 1 || c.Port > 65535 {
		return c, invalidf("container port must be between 1 and 65535")
	}
	if c.HealthPath != "" && !strings.HasPrefix(c.HealthPath, "/") {
		return c, invalidf("health path must start with /")
	}
	if c.GPUs < 0 {
		return c, invalidf("container GPU count must not be negative")
	}
	if c.NoGPU && c.GPUs != 0 {
		return c, invalidf("a container that needs no GPU cannot also ask for some")
	}
	if c.CachePath != "" && !strings.HasPrefix(c.CachePath, "/") {
		return c, invalidf("cache path must be an absolute path inside the container")
	}
	return c, nil
}

func (s *Service) validateApp(ctx context.Context, in AppInput) (AppInput, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" || len(in.Name) > 200 || strings.IndexFunc(in.Name, unicode.IsControl) >= 0 {
		return in, invalidf("app name must be 1 to 200 printable characters")
	}
	if in.Mode != ModePersistent && in.Mode != ModeSingleShot {
		return in, invalidf("mode must be %q or %q", ModePersistent, ModeSingleShot)
	}
	price, err := validatePrice(in.Price)
	if err != nil {
		return in, err
	}
	in.Price = price
	in.RequiredVersion = strings.TrimSpace(in.RequiredVersion)
	if in.RequiredVersion == "" || len(in.RequiredVersion) > 100 {
		return in, invalidf("required version must be 1 to 100 characters")
	}

	if in.Container, err = validateContainer(in.Container); err != nil {
		return in, err
	}

	if in.DefaultCapacity < 1 || in.MaxCapacity < in.DefaultCapacity {
		return in, invalidf("capacity must satisfy 1 ≤ default ≤ maximum")
	}
	if in.EligibleGPUs == nil {
		in.EligibleGPUs = []string{}
	}
	for _, id := range in.EligibleGPUs {
		if _, err := s.gpu(ctx, id); err != nil {
			return in, invalidf("eligible GPU %s is not in the supported list", id)
		}
	}

	if in.Test.Defined() {
		t := &in.Test
		t.Method = strings.ToUpper(strings.TrimSpace(t.Method))
		if t.Method == "" {
			t.Method = "GET"
		}
		if !strings.HasPrefix(t.Path, "/") {
			return in, invalidf("test path must start with /")
		}
		if t.ExpectStatus < 100 || t.ExpectStatus > 599 {
			return in, invalidf("test must expect an HTTP status between 100 and 599")
		}
		if t.TimeoutMs <= 0 {
			return in, invalidf("test time limit must be positive")
		}
	} else {
		in.Test = Test{}
	}
	return in, nil
}

const appColumns = `id, name, mode, price, price_unit, required_version, previous_version, previous_version_deadline,
	container, eligible_gpus, default_capacity, max_capacity, test, discovery, description, offered, created_at, updated_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanApp(row rowScanner) (*App, error) {
	var a App
	var container, eligible, test string
	var deadline, created, updated int64
	if err := row.Scan(&a.ID, &a.Name, &a.Mode, &a.Price.Amount, &a.Price.Unit, &a.RequiredVersion, &a.PreviousVersion, &deadline,
		&container, &eligible, &a.DefaultCapacity, &a.MaxCapacity, &test, &a.Discovery, &a.Description, &a.Offered, &created, &updated); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(container), &a.Container); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(eligible), &a.EligibleGPUs); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(test), &a.Test); err != nil {
		return nil, err
	}
	a.PreviousVersionDeadline = optionalTime(deadline)
	a.CreatedAt, a.UpdatedAt = fromMs(created), fromMs(updated)
	return &a, nil
}

// Apps lists the catalog; onlyOffered leaves out paused apps.
func (s *Service) Apps(ctx context.Context, onlyOffered bool) ([]App, error) {
	query := `SELECT ` + appColumns + ` FROM apps`
	if onlyOffered {
		query += ` WHERE offered = 1`
	}
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	apps := []App{}
	for rows.Next() {
		a, err := scanApp(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *a)
	}
	return apps, rows.Err()
}

// App returns one app by id.
func (s *Service) App(ctx context.Context, id string) (*App, error) {
	a, err := scanApp(s.db.QueryRowContext(ctx, `SELECT `+appColumns+` FROM apps WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFoundf("app %s does not exist", id)
	}
	return a, err
}

func (s *Service) appByName(ctx context.Context, name string) (*App, error) {
	a, err := scanApp(s.db.QueryRowContext(ctx, `SELECT `+appColumns+` FROM apps WHERE name = ?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFoundf("app %q does not exist", name)
	}
	return a, err
}

// CreateApp adds an app to the catalog, offered at once.
func (s *Service) CreateApp(ctx context.Context, actor string, in AppInput) (*App, error) {
	in, err := s.validateApp(ctx, in)
	if err != nil {
		return nil, err
	}
	id, now := newID("app"), s.nowMs()
	_, err = s.db.ExecContext(ctx, `INSERT INTO apps (id, name, mode, price, price_unit, required_version, container, eligible_gpus,
		default_capacity, max_capacity, test, discovery, description, offered, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		id, in.Name, in.Mode, in.Price.Amount, in.Price.Unit, in.RequiredVersion, mustJSON(in.Container), mustJSON(in.EligibleGPUs),
		in.DefaultCapacity, in.MaxCapacity, mustJSON(in.Test), in.Discovery, in.Description, now, now)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, conflictf("an app named %q already exists", in.Name)
		}
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "app.create", id, in.Name); err != nil {
		return nil, err
	}
	s.pushPricesSoon(ctx)
	return s.App(ctx, id)
}

// UpdateApp changes an app. Its name, required version and container stay
// as they are.
func (s *Service) UpdateApp(ctx context.Context, actor, id string, in AppInput) (*App, error) {
	current, err := s.App(ctx, id)
	if err != nil {
		return nil, err
	}
	if in.Name != "" && strings.TrimSpace(in.Name) != current.Name {
		return nil, invalidf("an app cannot be renamed: runners and prices are known by its name")
	}
	if in.RequiredVersion != "" && strings.TrimSpace(in.RequiredVersion) != current.RequiredVersion {
		return nil, invalidf("change the required version with the version action, which gives runners on the old one a deadline")
	}
	in.Name, in.RequiredVersion = current.Name, current.RequiredVersion
	if in.Container.Image != "" && mustJSON(in.Container) != mustJSON(current.Container) {
		return nil, invalidf("change the container with the version action: an app's image and version change together")
	}
	in.Container = current.Container
	if in.Mode != current.Mode {
		return nil, invalidf("an app's mode cannot change; create another app")
	}
	in, err = s.validateApp(ctx, in)
	if err != nil {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE apps SET price = ?, price_unit = ?, eligible_gpus = ?, default_capacity = ?,
		max_capacity = ?, test = ?, discovery = ?, description = ?, updated_at = ? WHERE id = ?`,
		in.Price.Amount, in.Price.Unit, mustJSON(in.EligibleGPUs), in.DefaultCapacity,
		in.MaxCapacity, mustJSON(in.Test), in.Discovery, in.Description, s.nowMs(), id)
	if err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "app.update", id, current.Name); err != nil {
		return nil, err
	}
	s.pushPricesSoon(ctx)
	return s.App(ctx, id)
}

// SetAppVersion releases a new version of an app: the version runners must
// report and the container that is that version, together. Machines are
// moved to it a few at a time, each after its sessions have finished. A
// runner that registers by itself must match at once; the version it
// replaces stays admitted until deadline, which defaults to the pool's
// version deadline from now. A nil container keeps the one the app has.
func (s *Service) SetAppVersion(ctx context.Context, actor, id, version string, container *Container, deadline *time.Time) (*App, error) {
	current, err := s.App(ctx, id)
	if err != nil {
		return nil, err
	}
	version = strings.TrimSpace(version)
	if version == "" || len(version) > 100 {
		return nil, invalidf("required version must be 1 to 100 characters")
	}
	if version == current.RequiredVersion {
		return nil, conflictf("%s already requires version %s", current.Name, version)
	}
	next := current.Container
	if container != nil {
		if next, err = validateContainer(*container); err != nil {
			return nil, err
		}
	}
	until := s.now()
	if deadline != nil {
		until = deadline.UTC()
	} else {
		settings, err := s.Settings(ctx)
		if err != nil {
			return nil, err
		}
		until = until.Add(time.Duration(settings.VersionDeadlineHours) * time.Hour)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE apps SET required_version = ?, container = ?, previous_version = ?, previous_version_deadline = ?, updated_at = ? WHERE id = ?`,
		version, mustJSON(next), current.RequiredVersion, until.UnixMilli(), s.nowMs(), id); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, s.db, actor, "app.version", id, current.RequiredVersion+" -> "+version); err != nil {
		return nil, err
	}
	return s.App(ctx, id)
}

// SetAppOffered pauses or resumes an app. A paused app admits no runners and
// is not priced on the orchestrator, so customers cannot ask for it.
func (s *Service) SetAppOffered(ctx context.Context, actor, id string, offered bool) (*App, error) {
	if _, err := s.App(ctx, id); err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE apps SET offered = ?, updated_at = ? WHERE id = ?`, offered, s.nowMs(), id); err != nil {
		return nil, err
	}
	action := "app.pause"
	if offered {
		action = "app.resume"
	}
	if err := s.audit(ctx, s.db, actor, action, id, ""); err != nil {
		return nil, err
	}
	s.pushPricesSoon(ctx)
	return s.App(ctx, id)
}

// versionAdmitted says whether a runner reporting version may register for the app now.
func (a *App) versionAdmitted(version string, now time.Time) bool {
	if version == a.RequiredVersion {
		return true
	}
	return a.PreviousVersion != "" && version == a.PreviousVersion &&
		a.PreviousVersionDeadline != nil && now.Before(*a.PreviousVersionDeadline)
}

// PushPrices sends the offered apps' prices to the orchestrator, replacing
// its price table. The pool's catalog is the source of the orchestrator's prices.
func (s *Service) PushPrices(ctx context.Context) error {
	if s.orch == nil {
		return nil
	}
	apps, err := s.Apps(ctx, true)
	if err != nil {
		return err
	}
	prices := make(map[string]Price, len(apps))
	for _, a := range apps {
		prices[a.Name] = a.Price
	}
	if err := s.orch.SetPrices(ctx, prices); err != nil {
		return unavailablef("the orchestrator did not take the prices: %v", err)
	}
	return nil
}

// pushPricesSoon pushes prices after a catalog change. A failure is logged:
// the change is saved, and the periodic push delivers it once the
// orchestrator answers.
func (s *Service) pushPricesSoon(ctx context.Context) {
	if err := s.PushPrices(ctx); err != nil {
		s.log.Warn("prices not pushed to the orchestrator yet", "err", err)
	}
}
