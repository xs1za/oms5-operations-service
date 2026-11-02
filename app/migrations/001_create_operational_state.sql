CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(100) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Client hierarchy: node types reference
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS client_node_types (
    code VARCHAR(32) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

INSERT INTO client_node_types (code, name) VALUES
    ('head', 'Головной клиент'),
    ('division', 'Дивизион/область'),
    ('object', 'Магазин/объект')
ON CONFLICT (code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- clients: self-referencing hierarchy, three levels
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clients (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    external_id VARCHAR(255),
    client_type_code VARCHAR(32) NOT NULL REFERENCES client_node_types(code),
    parent_client_id VARCHAR(36) REFERENCES clients(id),
    created_at TIMESTAMPTZ NOT NULL
);

-- Upgrade an existing clients table that predates the hierarchy columns.
ALTER TABLE clients ADD COLUMN IF NOT EXISTS client_type_code VARCHAR(32) NOT NULL DEFAULT 'object';
ALTER TABLE clients ADD COLUMN IF NOT EXISTS parent_client_id VARCHAR(36);
UPDATE clients SET client_type_code = 'object' WHERE client_type_code IS NULL;

CREATE INDEX IF NOT EXISTS ix_clients_parent ON clients (parent_client_id);

-- Store-level location data so a shift's country, timezone and address resolve via clients.client_id.
ALTER TABLE clients ADD COLUMN IF NOT EXISTS address VARCHAR(500);
ALTER TABLE clients ADD COLUMN IF NOT EXISTS country_code CHAR(2);
ALTER TABLE clients ADD COLUMN IF NOT EXISTS timezone VARCHAR(64);
ALTER TABLE clients ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;

CREATE UNIQUE INDEX IF NOT EXISTS uq_clients_external_id
    ON clients (external_id) WHERE external_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- performers: employee_id renamed to person_id (no re-keying)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS performers (
    id VARCHAR(36) PRIMARY KEY,
    person_id BIGINT NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(100),
    status VARCHAR(50) NOT NULL,
    source_version BIGINT,
    synced_at TIMESTAMPTZ NOT NULL,
    merged_into_performer_id VARCHAR(36) REFERENCES performers(id)
);

-- Idempotent rename for databases created before the person_id contract.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'performers' AND column_name = 'employee_id')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'performers' AND column_name = 'person_id') THEN
        ALTER TABLE performers RENAME COLUMN employee_id TO person_id;
    END IF;
END $$;

ALTER TABLE performers ADD COLUMN IF NOT EXISTS merged_into_performer_id VARCHAR(36);

CREATE UNIQUE INDEX IF NOT EXISTS uq_performers_canonical_person
    ON performers (person_id) WHERE merged_into_performer_id IS NULL;

-- ---------------------------------------------------------------------------
-- shifts with author snapshots
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shifts (
    id VARCHAR(36) PRIMARY KEY,
    client_id VARCHAR(36) NOT NULL REFERENCES clients(id),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    location VARCHAR(255),
    required_performers_count INTEGER NOT NULL DEFAULT 1 CHECK (required_performers_count = 1),
    created_by_user_id VARCHAR(255),
    created_by_person_id BIGINT,
    created_by_display_name VARCHAR(255),
    status VARCHAR(30) NOT NULL,
    assigned_performer_id VARCHAR(36) REFERENCES performers(id),
    assigned_performer_person_id BIGINT,
    close_reason VARCHAR(50),
    failure_reason VARCHAR(100),
    auto_closed BOOLEAN NOT NULL DEFAULT FALSE,
    auto_close_reason VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (ends_at > starts_at)
);

ALTER TABLE shifts ADD COLUMN IF NOT EXISTS created_by_person_id BIGINT;
ALTER TABLE shifts ADD COLUMN IF NOT EXISTS created_by_display_name VARCHAR(255);
ALTER TABLE shifts ADD COLUMN IF NOT EXISTS assigned_performer_person_id BIGINT;

CREATE INDEX IF NOT EXISTS ix_shifts_starts_at ON shifts (starts_at);
CREATE INDEX IF NOT EXISTS ix_shifts_status ON shifts (status);

-- ---------------------------------------------------------------------------
-- shift_status_history with actor snapshots
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shift_status_history (
    id VARCHAR(36) PRIMARY KEY,
    shift_id VARCHAR(36) NOT NULL REFERENCES shifts(id) ON DELETE CASCADE,
    from_status VARCHAR(30) NOT NULL,
    to_status VARCHAR(30) NOT NULL,
    transition_code VARCHAR(100) NOT NULL,
    reason VARCHAR(255),
    actor_user_id VARCHAR(255),
    actor_person_id BIGINT,
    actor_display_name VARCHAR(255),
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id VARCHAR(36) NOT NULL UNIQUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE shift_status_history ADD COLUMN IF NOT EXISTS actor_person_id BIGINT;
ALTER TABLE shift_status_history ADD COLUMN IF NOT EXISTS actor_display_name VARCHAR(255);

CREATE INDEX IF NOT EXISTS ix_shift_status_history_shift ON shift_status_history (shift_id, occurred_at);

-- ---------------------------------------------------------------------------
-- offer campaigns with author snapshots
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS offer_campaigns (
    id VARCHAR(36) PRIMARY KEY,
    shift_id VARCHAR(36) NOT NULL REFERENCES shifts(id) ON DELETE CASCADE,
    status VARCHAR(30) NOT NULL,
    created_by_user_id VARCHAR(255),
    created_by_person_id BIGINT,
    created_by_display_name VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE offer_campaigns ADD COLUMN IF NOT EXISTS created_by_person_id BIGINT;
ALTER TABLE offer_campaigns ADD COLUMN IF NOT EXISTS created_by_display_name VARCHAR(255);

-- ---------------------------------------------------------------------------
-- offers, views, assignments, timesheets
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shift_offers (
    id VARCHAR(36) PRIMARY KEY,
    campaign_id VARCHAR(36) NOT NULL REFERENCES offer_campaigns(id) ON DELETE CASCADE,
    shift_id VARCHAR(36) NOT NULL REFERENCES shifts(id) ON DELETE CASCADE,
    performer_id VARCHAR(36) NOT NULL REFERENCES performers(id),
    status VARCHAR(30) NOT NULL,
    token_hash VARCHAR(64) NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    responded_at TIMESTAMPTZ,
    UNIQUE (campaign_id, performer_id)
);

CREATE INDEX IF NOT EXISTS ix_shift_offers_shift_status ON shift_offers (shift_id, status);

CREATE TABLE IF NOT EXISTS shift_offer_views (
    id VARCHAR(36) PRIMARY KEY,
    offer_id VARCHAR(36) NOT NULL REFERENCES shift_offers(id) ON DELETE CASCADE,
    viewed_at TIMESTAMPTZ NOT NULL,
    ip_address VARCHAR(255),
    user_agent VARCHAR(1000),
    correlation_id VARCHAR(36) NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS ix_shift_offer_views_offer ON shift_offer_views (offer_id, viewed_at);

CREATE TABLE IF NOT EXISTS shift_assignments (
    id VARCHAR(36) PRIMARY KEY,
    shift_id VARCHAR(36) NOT NULL REFERENCES shifts(id) ON DELETE CASCADE,
    performer_id VARCHAR(36) NOT NULL REFERENCES performers(id),
    offer_id VARCHAR(36) NOT NULL UNIQUE REFERENCES shift_offers(id),
    status VARCHAR(30) NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL,
    removed_at TIMESTAMPTZ,
    reason VARCHAR(255)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_shift_assignment
    ON shift_assignments (shift_id) WHERE status = 'assigned';

DO $$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS uq_shifts_client_start
        ON shifts (client_id, starts_at);
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'uq_shifts_client_start skipped: duplicate (client_id, starts_at) rows exist';
END $$;

CREATE TABLE IF NOT EXISTS timesheets (
    id VARCHAR(36) PRIMARY KEY,
    performer_id VARCHAR(36) NOT NULL REFERENCES performers(id),
    shift_id VARCHAR(36) NOT NULL REFERENCES shifts(id) ON DELETE CASCADE,
    work_date DATE NOT NULL,
    hours DOUBLE PRECISION NOT NULL CHECK (hours > 0 AND hours <= 24),
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_timesheets_shift ON timesheets (shift_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_timesheet_performer_shift_date
    ON timesheets (performer_id, shift_id, work_date);

CREATE TABLE IF NOT EXISTS person_event_inbox (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id BIGINT NOT NULL,
    aggregate_version BIGINT NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (aggregate_id, aggregate_version)
);

CREATE INDEX IF NOT EXISTS ix_person_event_inbox_aggregate ON person_event_inbox (aggregate_id, aggregate_version);

CREATE TABLE IF NOT EXISTS person_consumer_state (
    aggregate_id BIGINT PRIMARY KEY,
    last_version BIGINT NOT NULL,
    last_event_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO schema_migrations (version)
VALUES ('001_create_operational_state')
ON CONFLICT (version) DO NOTHING;
