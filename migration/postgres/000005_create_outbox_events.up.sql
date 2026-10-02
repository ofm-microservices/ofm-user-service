CREATE TABLE IF NOT EXISTS outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    operation TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_aggregate
    ON outbox_events (aggregate_type, aggregate_id, created_at);

CREATE OR REPLACE FUNCTION capture_user_outbox_event() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE row_data JSONB; operation_value TEXT := CASE TG_OP WHEN 'INSERT' THEN 'created' WHEN 'DELETE' THEN 'deactivated' ELSE 'updated' END;
BEGIN
    row_data := CASE WHEN TG_OP = 'DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
    INSERT INTO outbox_events(event_id, aggregate_type, aggregate_id, event_type, operation, payload)
    VALUES (md5(clock_timestamp()::TEXT || random()::TEXT)::UUID, 'user', row_data->>'user_id', 'user.profile.' || operation_value, operation_value, row_data);
    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$$;

DROP TRIGGER IF EXISTS users_outbox ON users;
CREATE TRIGGER users_outbox AFTER INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE FUNCTION capture_user_outbox_event();
