-- Extend raw metrics retention from 1 hour to 7 days so load tests and
-- ad-hoc raw-metric queries can span a usable window. Rollups (_1m/_5m/_1h)
-- keep their own retention; this only changes the raw fact table.
ALTER TABLE observability.metrics MODIFY TTL timestamp + INTERVAL 7 DAY DELETE;
