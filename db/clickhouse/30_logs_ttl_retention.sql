-- Relax observability.logs TTL from legacy 1h (dev) to 30d so UI queries match retained data.
ALTER TABLE observability.logs
    MODIFY TTL timestamp + INTERVAL 30 DAY DELETE;
