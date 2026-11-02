-- ---------------------------------------------------------------------------
-- 002: performer funnel projection, assignment engagement snapshot, outbox
-- ---------------------------------------------------------------------------

ALTER TABLE performers
    ADD COLUMN IF NOT EXISTS engagement_type VARCHAR(32),
    ADD COLUMN IF NOT EXISTS engagement_type_validation_status VARCHAR(16),
    ADD COLUMN IF NOT EXISTS funnel_stage VARCHAR(40),
    ADD COLUMN IF NOT EXISTS medical_book_status VARCHAR(16),
    ADD COLUMN IF NOT EXISTS completed_shifts_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_booked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS first_shift_completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fifth_shift_completed_at TIMESTAMPTZ;

ALTER TABLE shift_assignments
    ADD COLUMN IF NOT EXISTS engagement_type VARCHAR(32),
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'performers_engagement_type_ck') THEN
        ALTER TABLE performers
            ADD CONSTRAINT performers_engagement_type_ck
            CHECK (engagement_type IS NULL OR engagement_type IN ('gph', 'self_employed'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'performers_validation_status_ck') THEN
        ALTER TABLE performers
            ADD CONSTRAINT performers_validation_status_ck
            CHECK (engagement_type_validation_status IS NULL
                   OR engagement_type_validation_status IN ('pending', 'verified', 'rejected'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'performers_medical_book_ck') THEN
        ALTER TABLE performers
            ADD CONSTRAINT performers_medical_book_ck
            CHECK (medical_book_status IS NULL
                   OR medical_book_status IN ('none', 'non_food', 'food'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'performers_funnel_stage_ck') THEN
        ALTER TABLE performers
            ADD CONSTRAINT performers_funnel_stage_ck
            CHECK (funnel_stage IS NULL OR funnel_stage IN (
                'PERFORMER_BLANK',
                'PERFORMER_PROFILE',
                'PERFORMER_DOCS_PENDING',
                'PERFORMER_READY_NON_MEDICAL',
                'PERFORMER_READY_NON_FOOD',
                'PERFORMER_READY_FOOD',
                'PERFORMER_BOOKED_SHIFT',
                'PERFORMER_FIRST_SHIFT_COMPLETED',
                'PERFORMER_FIFTH_SHIFT_COMPLETED',
                'PERFORMER_PROFILE_DELETED'
            ));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'performers_completed_shifts_ck') THEN
        ALTER TABLE performers
            ADD CONSTRAINT performers_completed_shifts_ck
            CHECK (completed_shifts_count >= 0);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shift_assignments_engagement_type_ck') THEN
        ALTER TABLE shift_assignments
            ADD CONSTRAINT shift_assignments_engagement_type_ck
            CHECK (engagement_type IS NULL OR engagement_type IN ('gph', 'self_employed'));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS operations_outbox (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(80) NOT NULL,
    aggregate_type VARCHAR(32) NOT NULL DEFAULT 'performer',
    aggregate_id VARCHAR(32) NOT NULL,
    person_id BIGINT NOT NULL,
    assignment_id UUID,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR(500)
);

CREATE INDEX IF NOT EXISTS ix_operations_outbox_pending
    ON operations_outbox (created_at, id)
    WHERE published_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_operations_outbox_assignment_event
    ON operations_outbox (event_type, assignment_id)
    WHERE assignment_id IS NOT NULL;

INSERT INTO schema_migrations (version)
VALUES ('002_performer_funnel_and_outbox')
ON CONFLICT (version) DO NOTHING;
