CREATE INDEX "index_block_table_block_hash" ON "block" ("block_hash");

CREATE INDEX "index_tx_table_tx_hash" ON "ckb_transaction" ("tx_hash");
CREATE INDEX "index_tx_table_block_id" ON "ckb_transaction" ("block_id");

CREATE INDEX "idx_output_table_tx_id_output_index" ON "output" ("tx_id", "output_index");
CREATE INDEX "idx_output_table_lock_script_id" ON "output" ("lock_script_id");
CREATE INDEX "idx_output_table_type_script_id" ON "output" ("type_script_id");

CREATE INDEX "idx_input_table_consumed_tx_id" ON "input" ("consumed_tx_id");

CREATE INDEX "idx_udt_output_table_tx_id_output_index" ON "udt_output" ("tx_id", "output_index");

CREATE INDEX "idx_dob_output_table_tx_id_output_index" ON "dob_output" ("tx_id", "output_index");
CREATE INDEX "idx_dob_output_table_lock_script_id" ON "dob_output" ("spore_id");

CREATE INDEX "idx_cluster_output_table_tx_id_output_index" ON "cluster_output" ("tx_id", "output_index");
CREATE INDEX "idx_cluster_output_table_lock_script_id" ON "cluster_output" ("cluster_id");

CREATE INDEX "idx_dob_table_cluster_id" ON "dob" ("cluster_id");