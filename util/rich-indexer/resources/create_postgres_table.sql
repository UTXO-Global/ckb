CREATE TABLE block(
    id BIGSERIAL PRIMARY KEY,
    block_hash BYTEA NOT NULL,
    block_number BIGINT NOT NULL
);

CREATE TABLE ckb_transaction(
    id BIGSERIAL PRIMARY KEY,
    tx_hash BYTEA NOT NULL,
    block_id BIGINT NOT NULL,
    tx_index INTEGER NOT NULL
);

CREATE TABLE output(
    id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    output_index INTEGER NOT NULL,
    capacity BIGINT NOT NULL,
    lock_script_id BIGINT,
    type_script_id BIGINT,
    data BYTEA
);

CREATE TABLE input(
    output_id BIGINT PRIMARY KEY,
    since BYTEA NOT NULL,
    consumed_tx_id BIGINT NOT NULL,
    input_index INTEGER NOT NULL
);

CREATE TABLE script(
    id BIGSERIAL PRIMARY KEY,
    code_hash BYTEA NOT NULL,
    hash_type SMALLINT NOT NULL,
    args BYTEA,
    UNIQUE(code_hash, hash_type, args)
);

CREATE TABLE udt(
    type_script_id BIGINT PRIMARY KEY,
    data BYTEA, -- decimal, name, symbol
    type SMALLINT NOT NULL -- xudt or sudt
);

CREATE TABLE udt_output(
    id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    output_index INTEGER NOT NULL,
    amount BYTEA NOT NULL
);

CREATE TABLE dob(
    spore_id BYTEA PRIMARY KEY,
    content_type BYTEA NOT NULL,
    content BYTEA NOT NULL,
    cluster_id BYTEA NOT NULL
);

CREATE TABLE dob_output(
    id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    output_index INTEGER NOT NULL,
    spore_id BYTEA NOT NULL
);

CREATE TABLE cluster(
    cluster_id BYTEA PRIMARY KEY,
    name BYTEA NOT NULL,
    description BYTEA NOT NULL
);

CREATE TABLE cluster_output(
    id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    output_index INTEGER NOT NULL,
    cluster_id BYTEA NOT NULL
);
