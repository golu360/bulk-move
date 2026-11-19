-- Failed attempts at the job's current chunk: a chunk error, or a worker
-- that died holding the lease (counted when another worker takes over).
-- Committed progress resets it to 0; reaching the worker's max fails the job.
ALTER TABLE bulk_jobs ADD COLUMN attempts int NOT NULL DEFAULT 0;
