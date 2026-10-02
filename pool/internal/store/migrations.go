package store

// migrations are applied in order, each once. Never edit one that has
// shipped; add another. Times are Unix milliseconds; JSON columns hold the
// values the core package defines.
var migrations = []string{
	// 1: catalog, people, machines, sign-in, audit.
	`
CREATE TABLE settings (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE gpus (
	id         TEXT PRIMARY KEY,
	name       TEXT NOT NULL UNIQUE,
	vram_mb    INTEGER NOT NULL,
	accepted   INTEGER NOT NULL DEFAULT 1,
	created_at INTEGER NOT NULL
);

CREATE TABLE apps (
	id                        TEXT PRIMARY KEY,
	name                      TEXT NOT NULL UNIQUE,
	mode                      TEXT NOT NULL,
	price                     TEXT NOT NULL,
	price_unit                TEXT NOT NULL,
	required_version          TEXT NOT NULL,
	previous_version          TEXT NOT NULL DEFAULT '',
	previous_version_deadline INTEGER NOT NULL DEFAULT 0,
	container                 TEXT NOT NULL DEFAULT '{}',
	eligible_gpus             TEXT NOT NULL DEFAULT '[]',
	default_capacity          INTEGER NOT NULL,
	max_capacity              INTEGER NOT NULL,
	test                      TEXT NOT NULL DEFAULT '{}',
	discovery                 TEXT NOT NULL DEFAULT '',
	description               TEXT NOT NULL DEFAULT '',
	offered                   INTEGER NOT NULL DEFAULT 1,
	created_at                INTEGER NOT NULL,
	updated_at                INTEGER NOT NULL
);

CREATE TABLE terms (
	scope        TEXT NOT NULL CHECK (scope IN ('global', 'regional')),
	version      INTEGER NOT NULL,
	body         TEXT NOT NULL,
	hash         TEXT NOT NULL,
	published_at INTEGER NOT NULL,
	PRIMARY KEY (scope, version)
);

CREATE TABLE applications (
	id                     TEXT PRIMARY KEY,
	address                TEXT NOT NULL,
	display_name           TEXT NOT NULL,
	machines               TEXT NOT NULL,
	global_terms_version   INTEGER NOT NULL,
	global_terms_hash      TEXT NOT NULL,
	regional_terms_version INTEGER NOT NULL DEFAULT 0,
	regional_terms_hash    TEXT NOT NULL DEFAULT '',
	message                TEXT NOT NULL,
	signature              TEXT NOT NULL,
	status                 TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected')),
	reason                 TEXT NOT NULL DEFAULT '',
	created_at             INTEGER NOT NULL,
	decided_at             INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX applications_address ON applications (address);
CREATE INDEX applications_status ON applications (status);

CREATE TABLE members (
	id           TEXT PRIMARY KEY,
	address      TEXT NOT NULL UNIQUE,
	display_name TEXT NOT NULL,
	status       TEXT NOT NULL CHECK (status IN ('active', 'banned', 'left')),
	reason       TEXT NOT NULL DEFAULT '',
	created_at   INTEGER NOT NULL,
	updated_at   INTEGER NOT NULL
);

CREATE TABLE machines (
	id           TEXT PRIMARY KEY,
	member_id    TEXT NOT NULL REFERENCES members (id),
	label        TEXT NOT NULL,
	gpus         TEXT NOT NULL,
	reported     TEXT NOT NULL DEFAULT '{}',
	status       TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'removed')),
	created_at   INTEGER NOT NULL,
	last_seen_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX machines_member ON machines (member_id);

CREATE TABLE assignments (
	id         TEXT PRIMARY KEY,
	machine_id TEXT NOT NULL REFERENCES machines (id),
	app_id     TEXT NOT NULL REFERENCES apps (id),
	capacity   INTEGER NOT NULL,
	status     TEXT NOT NULL CHECK (status IN ('active', 'draining', 'removed')),
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
-- A machine runs an app at most once.
CREATE UNIQUE INDEX assignments_machine_app ON assignments (machine_id, app_id) WHERE status != 'removed';

CREATE TABLE credentials (
	id          TEXT PRIMARY KEY,
	machine_id  TEXT NOT NULL REFERENCES machines (id),
	secret_hash TEXT NOT NULL UNIQUE,
	created_at  INTEGER NOT NULL,
	revoked_at  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX credentials_machine ON credentials (machine_id);

-- Runners this service admitted, so a runner on the orchestrator can be
-- traced to its machine.
CREATE TABLE runners (
	runner_id     TEXT PRIMARY KEY,
	machine_id    TEXT NOT NULL REFERENCES machines (id),
	assignment_id TEXT NOT NULL REFERENCES assignments (id),
	app           TEXT NOT NULL,
	version       TEXT NOT NULL,
	remote_addr   TEXT NOT NULL DEFAULT '',
	admitted_at   INTEGER NOT NULL,
	gone_at       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX runners_machine ON runners (machine_id);

CREATE TABLE nonces (
	nonce      TEXT PRIMARY KEY,
	expires_at INTEGER NOT NULL
);

CREATE TABLE admin_codes (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	code_hash  TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	expires_at INTEGER NOT NULL,
	attempts   INTEGER NOT NULL DEFAULT 0,
	used       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE audit (
	id      INTEGER PRIMARY KEY AUTOINCREMENT,
	at      INTEGER NOT NULL,
	actor   TEXT NOT NULL,
	action  TEXT NOT NULL,
	subject TEXT NOT NULL DEFAULT '',
	detail  TEXT NOT NULL DEFAULT ''
);
`,
	// 2: what the pool does with a runner once it is registered: tests it,
	// and promotes, suspends, holds or drains it.
	`
ALTER TABLE runners ADD COLUMN hold INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runners ADD COLUMN hold_reason TEXT NOT NULL DEFAULT '';
-- Why this service suspended the runner: 'tests', 'version', 'draining', or '' when it did not.
ALTER TABLE runners ADD COLUMN suspend_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE runners ADD COLUMN failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runners ADD COLUMN passes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runners ADD COLUMN last_test_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runners ADD COLUMN last_test_ok INTEGER NOT NULL DEFAULT 0;

CREATE TABLE test_results (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	runner_id  TEXT NOT NULL,
	machine_id TEXT NOT NULL,
	app        TEXT NOT NULL,
	at         INTEGER NOT NULL,
	ok         INTEGER NOT NULL,
	latency_ms INTEGER NOT NULL,
	detail     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX test_results_runner ON test_results (runner_id, id);
CREATE INDEX test_results_machine ON test_results (machine_id, id);
`,
}
