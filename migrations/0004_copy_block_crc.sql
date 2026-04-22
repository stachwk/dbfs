CREATE TABLE IF NOT EXISTS copy_block_crc (
    id_file INTEGER NOT NULL REFERENCES files(id_file) ON DELETE CASCADE,
    _order INTEGER NOT NULL,
    crc32 BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id_file, _order)
);
