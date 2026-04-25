CREATE TABLE IF NOT EXISTS data_objects (
    id_data_object SERIAL PRIMARY KEY,
    file_size BIGINT NOT NULL DEFAULT 0,
    content_hash BYTEA,
    reference_count INTEGER NOT NULL DEFAULT 1,
    creation_date TIMESTAMP NOT NULL DEFAULT NOW(),
    modification_date TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE files
    ADD COLUMN IF NOT EXISTS data_object_id INTEGER;

ALTER TABLE data_blocks
    ADD COLUMN IF NOT EXISTS data_object_id INTEGER;

ALTER TABLE copy_block_crc
    ADD COLUMN IF NOT EXISTS data_object_id INTEGER;

INSERT INTO data_objects (id_data_object, file_size, content_hash, reference_count, creation_date, modification_date)
SELECT id_file, size, NULL, 1, NOW(), NOW()
FROM files
WHERE data_object_id IS NULL
ON CONFLICT (id_data_object) DO NOTHING;

UPDATE files
SET data_object_id = id_file
WHERE data_object_id IS NULL;

UPDATE data_blocks
SET data_object_id = id_file
WHERE data_object_id IS NULL;

UPDATE copy_block_crc
SET data_object_id = id_file
WHERE data_object_id IS NULL;

ALTER TABLE files
    ALTER COLUMN data_object_id SET NOT NULL;

ALTER TABLE data_blocks
    ALTER COLUMN data_object_id SET NOT NULL;

ALTER TABLE copy_block_crc
    ALTER COLUMN data_object_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_files_data_object_id ON files (data_object_id);
CREATE INDEX IF NOT EXISTS idx_data_blocks_data_object_id ON data_blocks (data_object_id);
CREATE INDEX IF NOT EXISTS idx_copy_block_crc_data_object_id ON copy_block_crc (data_object_id);

DROP INDEX IF EXISTS idx_data_blocks_file_order;
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_blocks_object_order ON data_blocks (data_object_id, _order);
CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_block_crc_object_order ON copy_block_crc (data_object_id, _order);
