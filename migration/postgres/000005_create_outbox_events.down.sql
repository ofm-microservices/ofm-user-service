DROP TRIGGER IF EXISTS users_outbox ON users;
DROP FUNCTION IF EXISTS capture_user_outbox_event();
DROP TABLE IF EXISTS outbox_events;
