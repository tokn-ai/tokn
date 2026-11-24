-- Rename prompt_tok/completion_tok to input_tok/output_tok and add
-- cached_tok and reasoning_tok columns for breakdown of usage tokens.
ALTER TABLE requests RENAME COLUMN prompt_tok TO input_tok;
ALTER TABLE requests RENAME COLUMN completion_tok TO output_tok;
ALTER TABLE requests ADD COLUMN cached_tok INTEGER;
ALTER TABLE requests ADD COLUMN reasoning_tok INTEGER;
