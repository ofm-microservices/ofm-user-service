ALTER TABLE users
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS status TEXT;

UPDATE users
SET status = CASE
    WHEN is_active THEN 'active'
    ELSE 'pending_registration'
END
WHERE status IS NULL;

ALTER TABLE users
    ALTER COLUMN status SET NOT NULL;

ALTER TABLE users
    ALTER COLUMN status SET DEFAULT 'pending_registration';
