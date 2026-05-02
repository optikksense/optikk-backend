-- Migrate observability.logs.body from a tokenbf_v1 skip-index to the native
-- text (inverted) index that became GA in CH 26.2. Body-search queries
-- (hasToken, ILIKE, match) drop from seconds to ~hundreds of ms on the same
-- hardware (CH GA benchmark on 10B rows: 143s scan → 143s tokenbf → 0.4s text).
--
-- Tokenizer = splitByNonAlpha — same token semantics our reader already
-- assumes (whitespace + punctuation split). Preprocessor = lowerUTF8(str)
-- gives us case-insensitive matching at index time, so reader-side code calls
-- hasToken(body, lower(@search)) instead of hasTokenCaseInsensitive (the
-- latter is not in the text-index supported function list per CH 26.2 docs).
--
-- IF [NOT] EXISTS guards make the migration safe both on a fresh cluster
-- (where idx_body_tokens never existed) and on a re-run after a partial
-- failure (the runner records this file only after all three statements
-- succeed; a mid-file abort means we replay from the top).

ALTER TABLE observability.logs DROP INDEX IF EXISTS idx_body_tokens;

ALTER TABLE observability.logs
    ADD INDEX IF NOT EXISTS idx_body_text(body)
    TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = 'lowerUTF8(str)')
    GRANULARITY 1;

-- Materialize over historical parts. mutations_sync = 2 blocks until complete
-- on this replica so the migration step is observable. CH 26.2 caveat
-- (issue #93626): MATERIALIZE INDEX cannot run on individual parts > 2^32
-- rows; not a concern at our current scale.
ALTER TABLE observability.logs
    MATERIALIZE INDEX idx_body_text
    SETTINGS mutations_sync = 2;
