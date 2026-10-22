CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(100) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clients (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    external_id VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS performers (
    id VARCHAR(36) PRIMARY KEY,
    employee_id BIGINT NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(100),
    status VARCHAR(50) NOT NULL,
    source_version BIGINT,
    synced_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS shifts (
    id VARCHAR(36) PRIMARY KEY,
    client_id VARCHAR(36) NOT NULL REFERENCES clients(id),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    location VARCHAR(255),
    required_performers_count INTEGER NOT NULL DEFAULT 1 CHECK (required_performers_count = 1),
    created_by_user_id VARCHAR(255),
    status VARCHAR(30) NOT NULL,
    assigned_performer_id VARCHAR(36) REFERENCES performers(id),
    close_reason VARCHAR(50),
    failure_reason VARCHAR(100),
    auto_closed BOOLEAN NOT NULL DEFAULT FALSE,
    auto_close_reason VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (ends_at > starts_at)
);

CREATE INDEX IF NOT EXISTS ix_shifts_starts_at ON shifts (starts_at);
CREATE INDEX IF NOT EXISTS ix_shifts_status ON shifts (status);

CREATE TABLE IF NOT EXISTS shift_status_history (
    id VARCHAR(36) PRIMARY KEY,
    shift_id VARCHAR(36) NOT NULL REFERENCES shifts(id) ON DELETE CASCADE,
    from_status VARCHAR(30) NOT NULL,
    to_status VARCHAR(30) NOT NULL,
    transition_code VARCHAR(100) NOT NULL,
    reason VARCHAR(255),
    actor_user_id VARCHAR(255),
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id VARCHAR(36) NOT NULL UNIQUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_shift_status_history_shift ON shift_status_history (shift_id, occurred_at);

CREATE TABLE IF NOT EXISTS offer_campaigns (
    id VARCHAR(36) PRIMARY KEY,
    shift_id VARCHAR(36) NOT NULL REFERENCES shifts(id) ON DELETE CASCADE,
    status VARCHAR(30) NOT NULL,
    created_by_user_id VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL
);

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

INSERT INTO schema_migrations (version)
VALUES ('001_create_operational_state')
ON CONFLICT (version) DO NOTHING;
