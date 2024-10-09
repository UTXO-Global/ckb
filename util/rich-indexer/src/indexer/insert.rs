#![allow(clippy::needless_borrow)]

use super::{cluster_cell_data::ClusterCellData, spore_cell_data::SporeCellData, to_fixed_array};
use crate::store::SQLXPool;

use ckb_indexer_sync::Error;
use ckb_types::{
    bytes::Bytes,
    core::{BlockView, TransactionView},
    packed::{Byte, CellInput, CellOutput, OutPoint, ScriptBuilder},
    prelude::*,
};
use sql_builder::SqlBuilder;
use sqlx::{
    any::{Any, AnyArguments, AnyRow},
    query::Query,
    Row, Transaction,
};
use std::collections::HashSet;

// Note that every database has a practical limit on the number of bind parameters you can add to a single query.
// This varies by database.
// https://docs.rs/sqlx/0.6.3/sqlx/struct.QueryBuilder.html#note-database-specific-limits
// BATCH_SIZE_THRESHOLD represents the number of rows that can be bound in an insert sql execution.
// The number of columns in each row multiplied by this BATCH_SIZE_THRESHOLD yields the total number of bound parameters,
// which should be within the above limits.
pub(crate) const BATCH_SIZE_THRESHOLD: usize = 1_000;

type OutputCellRow = (
    i32,
    i64,
    (Vec<u8>, i16, Vec<u8>),
    Option<(Vec<u8>, i16, Vec<u8>)>,
    Vec<u8>,
);

enum FieldValue {
    Binary(Vec<u8>),
    BigInt(i64),
    Int(i32),
    NoneBigInt,
    SmallInt(i16),
}

impl FieldValue {
    fn bind<'a>(
        &'a self,
        query: Query<'a, Any, AnyArguments<'a>>,
    ) -> Query<'a, Any, AnyArguments<'a>> {
        match self {
            FieldValue::Binary(value) => query.bind(value),
            FieldValue::BigInt(value) => query.bind(value),
            FieldValue::Int(value) => query.bind(value),
            FieldValue::NoneBigInt => query.bind(Option::<i64>::None),
            FieldValue::SmallInt(value) => query.bind(value),
        }
    }
}

impl From<Vec<u8>> for FieldValue {
    fn from(value: Vec<u8>) -> Self {
        FieldValue::Binary(value)
    }
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        FieldValue::BigInt(value)
    }
}

impl From<i32> for FieldValue {
    fn from(value: i32) -> Self {
        FieldValue::Int(value)
    }
}

impl From<i16> for FieldValue {
    fn from(value: i16) -> Self {
        FieldValue::SmallInt(value)
    }
}

pub(crate) async fn append_block(
    block_view: &BlockView,
    tx: &mut Transaction<'_, Any>,
) -> Result<i64, Error> {
    // insert "uncle" first so that the row with the maximum ID in the "block" table corresponds to the tip block.
    let block_id = insert_block_table(block_view, tx).await?;
    Ok(block_id)
}

async fn insert_block_table(
    block_view: &BlockView,
    tx: &mut Transaction<'_, Any>,
) -> Result<i64, Error> {
    let block_row = block_view_to_field_values(block_view);
    bulk_insert_block_table(&[block_row], tx)
        .await
        .map(|ids| ids[0])
}

pub(crate) async fn insert_transaction_table(
    block_id: i64,
    tx_index: usize,
    tx_view: &TransactionView,
    tx: &mut Transaction<'_, Any>,
) -> Result<i64, Error> {
    let tx_row = vec![
        tx_view.hash().raw_data().to_vec().into(),
        block_id.into(),
        (tx_index as i32).into(),
    ];
    bulk_insert_and_return_ids(
        "ckb_transaction",
        &["tx_hash", "block_id", "tx_index"],
        &[tx_row],
        tx,
    )
    .await
    .map(|ids| ids[0])
}

pub(crate) async fn bulk_insert_blocks_simple(
    block_rows: Vec<(Vec<u8>, i64)>,
    tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    let simple_block_rows: Vec<Vec<FieldValue>> = block_rows
        .into_iter()
        .map(|(block_hash, block_number)| vec![block_hash.into(), block_number.into()])
        .collect();
    bulk_insert(
        "block",
        &["block_hash", "block_number"],
        &simple_block_rows,
        None,
        tx,
    )
    .await
}

async fn bulk_insert_block_table(
    block_rows: &[Vec<FieldValue>],
    tx: &mut Transaction<'_, Any>,
) -> Result<Vec<i64>, Error> {
    bulk_insert_and_return_ids("block", &["block_hash", "block_number"], block_rows, tx).await
}

pub(crate) async fn bulk_insert_output_table(
    tx_id: i64,
    output_cell_rows: Vec<OutputCellRow>,
    tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    let mut new_rows: Vec<Vec<FieldValue>> = Vec::new();
    // UDT variables
    let mut new_udt_rows: Vec<Vec<FieldValue>> = Vec::new();
    let mut new_xudt_type_script_ids: Vec<i64> = Vec::new();
    let mut new_unique_cells_data: Vec<Vec<u8>> = Vec::new();
    let mut new_udt_outputs: Vec<Vec<FieldValue>> = Vec::new();
    // NFT variables
    let mut new_dob_rows: Vec<Vec<FieldValue>> = Vec::new();
    let mut new_dob_outputs: Vec<Vec<FieldValue>> = Vec::new();
    let mut new_cluster_rows: Vec<Vec<FieldValue>> = Vec::new();
    let mut new_cluster_outputs: Vec<Vec<FieldValue>> = Vec::new();

    for row in output_cell_rows {
        let mut should_save_output = true;

        let type_script_id = if let Some(type_script) = &row.3 {
            let _type_script_id =
                query_script_id(&type_script.0, type_script.1, &type_script.2, tx).await?;

            if let Some(_type_script_id) = _type_script_id {
                let code_hash = type_script.0.clone();
                let arg = &type_script.2.clone();

                let code_hash_hex = hex::encode(&code_hash);
                match code_hash_hex.as_str() {
                        // ------------
                        // UDT
                        // Mainnet sudt
                        "5e7a36a77e68eecc013dfa2fe6a23f3b6c344b04005808694ae6dd45eea4cfd5"
                        // Testnet sudt
                        | "c5e5dcf215925f7ef4dfaf5f4b4f105bc321c02776d6e7d52a1db3fcd9d011a4" => {
                            let new_udt_row: Vec<FieldValue> = vec![
                                vec![].into(), // data
                                0.into(), // sudt type
                                _type_script_id.into() // type script id
                            ];
                            new_udt_rows.push(new_udt_row);

                            let udt_data = row.4.clone();
                            let bytes = if udt_data.len() > 16 {
                                16
                            } else {
                                udt_data.len()
                            };
                            let new_udt_output: Vec<FieldValue> = vec![
                                tx_id.into(), // tx_id
                                row.0.into(), // output_index
                                row.4.clone()[0..bytes].to_vec().into() // amount u128 - first 16 bytes, we cannot use bigint because of 8 bytes
                            ];
                            new_udt_outputs.push(new_udt_output);
                        }
                        // Mainnet + Testnet xudt
                        "50bd8d6680b8b9cf98b73f3c08faf8b2a21914311954118ad6609be6e78a1b95" 
                        // Testnet xudt(final_rls)
                        | "25c29dc317811a6f6f3985a7a9ebc4838bd388d19d0feeecf0bcd60f6c0975bb" // block: 8,497,330
                        => {
                            new_xudt_type_script_ids.push(_type_script_id);
                        }
                        // ------------
                        // Unique Cell
                        // Mainnet
                        "2c8c11c985da60b0a330c61a85507416d6382c130ba67f0c47ab071e00aec628"
                        // Testnet
                        | "8e341bcfec6393dcd41e635733ff2dca00a6af546949f70c57a706c0f344df8b" // block: 12,737,020
                        => {
                            new_unique_cells_data.push(row.4.clone());
                        }
                        // ------------
                        // NFT Cell
                        // DoB - Spore
                        // Mainnet
                        "4a4dce1df3dffff7f8b2cd7dff7303df3b6150c9788cb75dcf6747247132b9f5"
                        // Testnet
                        | "685a60219309029d01310311dba953d67029170ca4848a4ff638e57002130a0d" // block: 12,606,776
                        | "5e063b4c0e7abeaa6a428df3b693521a3050934cf3b0ae97a800d1bc31449398" // block: 11,994,104
                        | "bbad126377d45f90a8ee120da988a2d7332c78ba8fd679aab478a19d6c133494" // block: 10,228,288
                        => {
                            let spore_id = arg;
                            let reader = SporeCellData::from_slice(row.4.clone().as_slice());
                            if let Ok(spore_cell_data) = reader {
                                // spore_cell_data
                                let new_dob_row: Vec<FieldValue> = vec![
                                    spore_id.clone().into(), // spore_id
                                    spore_cell_data.content_type().as_slice().to_vec().into(), // content type
                                    spore_cell_data.content().as_slice().to_vec().into(), // content
                                    spore_cell_data.cluster_id().as_slice().to_vec().into() // cluster id
                                ];
                                new_dob_rows.push(new_dob_row);

                                let new_dob_output: Vec<FieldValue> = vec![
                                    tx_id.into(), // tx_id
                                    row.0.into(), // output_index
                                    spore_id.clone().into(), // spore_id
                                ];
                                new_dob_outputs.push(new_dob_output);
                            } else {
                                log::error!("parse spore data failed")
                            }
                        }
                        // DoB - Cluster
                        // https://github.com/sporeprotocol/spore-sdk/blob/83254c201f115c7bc4e3ac7638872a2ec4ca5671/packages/core/src/config/predefined.ts#L278
                        // https://github.com/nervosnetwork/ckb-explorer-frontend/blob/1c21cd5c1f11509f2a4fedf8503bc0a9e1276709/src/utils/spore.ts#L5
                        // e.g: https://pudge.explorer.nervos.org/transaction/0xac022fb5ab51a86e6dc6d0a45cad1fd4f9d2e7aad5a862a5003ca0cb8c7b21ea
                        // Mainnet
                        "7366a61534fa7c7e6225ecc0d828ea3b5366adec2b58206f2ee84995fe030075" |
                        // Testnet
                        "0bbe768b519d8ea7b96d58f1182eb7e6ef96c541fbd9526975077ee09f049058" // block: 12,606,811
                        => {
                            let cluster_id = arg;
                            let reader = ClusterCellData::from_slice(row.4.clone().as_slice());
                            if let Ok(cluster_cell_data) = reader {
                                // cluster_cell_data
                                let new_cluster_row: Vec<FieldValue> = vec![
                                    cluster_id.clone().into(), // cluster_id
                                    cluster_cell_data.name().as_slice().to_vec().into(), // name
                                    cluster_cell_data.description().as_slice().to_vec().into(), // description
                                ];
                                new_cluster_rows.push(new_cluster_row);

                                let new_cluster_output: Vec<FieldValue> = vec![
                                    tx_id.into(), // tx_id
                                    row.0.into(), // output_index
                                    cluster_id.clone().into(), // cluster_id
                                ];
                                new_cluster_outputs.push(new_cluster_output);
                            } else {
                                log::error!("parse cluster data failed")
                            }
                        }
                        _ => {
                            should_save_output = false;
                        }
                    };
            }

            _type_script_id
        } else {
            should_save_output = false;
            None
        };

        if should_save_output {
            let new_row = vec![
                tx_id.into(),
                row.0.into(),
                row.1.into(),
                query_script_id(&row.2 .0, row.2 .1, &row.2 .2, tx)
                    .await?
                    .map_or(FieldValue::NoneBigInt, FieldValue::BigInt),
                type_script_id.map_or(FieldValue::NoneBigInt, FieldValue::BigInt),
                row.4.into(),
            ];
            new_rows.push(new_row);
        }
    }

    // xUDT metadata will be update if there are xUDT cell and Unique cell
    // TODO: should check Unique Cell Data match xUDT metadata format define here https://github.com/ckb-cell/unique-cell
    for index in 0..new_xudt_type_script_ids.len() {
        let _type_script_id = *new_xudt_type_script_ids.get(index).unwrap();
        // Check if the index xUDT metadata hasn't been set
        let xudt_data = query_xudt_data(_type_script_id, tx).await?;
        if xudt_data.is_none() {
            if let Some(new_unique_cell_data) = new_unique_cells_data.pop() {
                let new_udt_row: Vec<FieldValue> = vec![
                    new_unique_cell_data.into(), // data
                    1.into(),                    // xudt type
                    _type_script_id.into(),      // type script id
                ];
                new_udt_rows.push(new_udt_row);
            }
        }
    }

    // UDT batch insert
    bulk_insert(
        "udt",
        &["data", "type", "type_script_id"],
        &new_udt_rows,
        Some(&["type_script_id"]),
        tx,
    )
    .await?;

    bulk_insert(
        "udt_output",
        &["tx_id", "output_index", "amount"],
        &new_udt_outputs,
        None,
        tx,
    )
    .await?;

    // NFT batch insert
    bulk_insert(
        "dob",
        &["spore_id", "content_type", "content", "cluster_id"],
        &new_dob_rows,
        Some(&["spore_id"]),
        tx,
    )
    .await?;

    bulk_insert(
        "dob_output",
        &["tx_id", "output_index", "spore_id"],
        &new_dob_outputs,
        None,
        tx,
    )
    .await?;

    bulk_insert(
        "cluster",
        &["cluster_id", "name", "description"],
        &new_cluster_rows,
        Some(&["cluster_id"]),
        tx,
    )
    .await?;

    bulk_insert(
        "cluster_output",
        &["tx_id", "output_index", "cluster_id"],
        &new_cluster_outputs,
        None,
        tx,
    )
    .await?;

    bulk_insert(
        "output",
        &[
            "tx_id",
            "output_index",
            "capacity",
            "lock_script_id",
            "type_script_id",
            "data",
        ],
        &new_rows,
        None,
        tx,
    )
    .await
}

pub(crate) async fn bulk_insert_input_table(
    tx_id: i64,
    input_rows: Vec<(i64, Vec<u8>, i32)>,
    tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    let input_rows = input_rows
        .into_iter()
        .map(|row| vec![row.0.into(), row.1.into(), tx_id.into(), row.2.into()])
        .collect::<Vec<Vec<FieldValue>>>();
    bulk_insert(
        "input",
        &["output_id", "since", "consumed_tx_id", "input_index"],
        &input_rows,
        Some(&["output_id"]),
        tx,
    )
    .await
}

pub(crate) async fn bulk_insert_script_table(
    script_set: HashSet<(Vec<u8>, i16, Vec<u8>)>,
    tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    // let script_rows = script_set.iter().collect::<Vec<_>>();
    let script_rows = script_set
        .into_iter()
        .map(|(code_hash, hash_type, args)| vec![code_hash.into(), hash_type.into(), args.into()])
        .collect::<Vec<_>>();
    bulk_insert(
        "script",
        &["code_hash", "hash_type", "args"],
        &script_rows,
        Some(&["code_hash", "hash_type", "args"]),
        tx,
    )
    .await
}

pub(crate) async fn spend_cell(
    out_point: &OutPoint,
    tx: &mut Transaction<'_, Any>,
) -> Result<bool, Error> {
    let output_tx_hash = out_point.tx_hash().raw_data().to_vec();
    let output_index: u32 = out_point.index().unpack();

    let updated_rows = sqlx::query(
        r#"
            UPDATE output
            SET is_spent = 1
            WHERE
                tx_id = (SELECT ckb_transaction.id FROM ckb_transaction WHERE tx_hash = $1)
                AND output_index = $2
        "#,
    )
    .bind(output_tx_hash)
    .bind(output_index as i32)
    .execute(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))?
    .rows_affected();

    Ok(updated_rows > 0)
}

pub(crate) async fn query_output_cell(
    out_point: &OutPoint,
    tx: &mut Transaction<'_, Any>,
) -> Result<Option<(i64, CellOutput, Bytes)>, Error> {
    let output_tx_hash = out_point.tx_hash().raw_data().to_vec();
    let output_index: u32 = out_point.index().unpack();

    let row = sqlx::query(
        r#"
        SELECT
            output.id,
            output.capacity,
            output.data,
            lock_script.code_hash AS lock_code_hash,
            lock_script.hash_type AS lock_hash_type,
            lock_script.args AS lock_args,
            type_script.code_hash AS type_code_hash,
            type_script.hash_type AS type_hash_type,
            type_script.args AS type_args
        FROM
            output
        LEFT JOIN
            script AS lock_script ON output.lock_script_id = lock_script.id
        LEFT JOIN
            script AS type_script ON output.type_script_id = type_script.id
        WHERE
            output.tx_id = (SELECT ckb_transaction.id FROM ckb_transaction WHERE tx_hash = $1)
            AND output.output_index = $2
        "#,
    )
    .bind(output_tx_hash)
    .bind(output_index as i32)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))?;

    Ok(build_cell_output(row))
}

pub(crate) async fn query_output_id(
    out_point: &OutPoint,
    tx: &mut Transaction<'_, Any>,
) -> Result<Option<i64>, Error> {
    let output_tx_hash = out_point.tx_hash().raw_data().to_vec();
    let output_index: u32 = out_point.index().unpack();

    sqlx::query(
        r#"
        SELECT output.id
        FROM
            output
        WHERE
            output.tx_id = (SELECT ckb_transaction.id FROM ckb_transaction WHERE tx_hash = $1)
            AND output.output_index = $2
        "#,
    )
    .bind(output_tx_hash)
    .bind(output_index as i32)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|row| row.map(|row| row.get::<i64, _>("id")))
}

pub(crate) async fn query_script_id(
    code_hash: &[u8],
    hash_type: i16,
    args: &[u8],
    tx: &mut Transaction<'_, Any>,
) -> Result<Option<i64>, Error> {
    sqlx::query(
        r#"
        SELECT id
        FROM
            script
        WHERE
            code_hash = $1 AND hash_type = $2 AND args = $3
        "#,
    )
    .bind(code_hash)
    .bind(hash_type)
    .bind(args)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|row| row.map(|row| row.get::<i64, _>("id")))
}

async fn query_xudt_data(
    type_script_id: i64,
    tx: &mut Transaction<'_, Any>,
) -> Result<Option<Vec<u8>>, Error> {
    sqlx::query(
        r#"
        SELECT
            data
        FROM
            udt
        WHERE
            type_script_id = $1
        "#,
    )
    .bind(type_script_id)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|row| row.map(|row| row.get::<Vec<u8>, _>("data")))
}

pub(crate) fn build_output_cell_rows(
    cell: &CellOutput,
    output_index: usize,
    data: &Bytes,
    output_cell_rows: &mut Vec<OutputCellRow>,
) {
    let cell_capacity: u64 = cell.capacity().unpack();
    let cell_row = (
        output_index as i32,
        cell_capacity as i64,
        (
            cell.lock().code_hash().raw_data().to_vec(),
            u8::from(cell.lock().hash_type()) as i16,
            cell.lock().args().raw_data().to_vec(),
        ),
        (cell.type_().to_opt().map(|type_script| {
            (
                type_script.code_hash().raw_data().to_vec(),
                u8::from(type_script.hash_type()) as i16,
                type_script.args().raw_data().to_vec(),
            )
        })),
        data.to_vec(),
    );
    output_cell_rows.push(cell_row);
}

pub(crate) async fn build_script_set(
    cell: &CellOutput,
    script_row: &mut HashSet<(Vec<u8>, i16, Vec<u8>)>,
) {
    let lock_script = cell.lock();
    let lock_script_row = (
        lock_script.code_hash().raw_data().to_vec(),
        u8::from(lock_script.hash_type()) as i16,
        lock_script.args().raw_data().to_vec(),
    );
    script_row.insert(lock_script_row);

    if let Some(type_script) = cell.type_().to_opt() {
        let type_script_row = (
            type_script.code_hash().raw_data().to_vec(),
            u8::from(type_script.hash_type()) as i16,
            type_script.args().raw_data().to_vec(),
        );
        script_row.insert(type_script_row);
    }
}

pub(crate) fn build_input_rows(
    output_id: i64,
    input: &CellInput,
    input_index: usize,
    input_rows: &mut Vec<(i64, Vec<u8>, i32)>,
) {
    let since: u64 = input.since().unpack();
    let input_row = (output_id, since.to_be_bytes().to_vec(), input_index as i32);
    input_rows.push(input_row);
}

fn build_cell_output(row: Option<AnyRow>) -> Option<(i64, CellOutput, Bytes)> {
    let row = match row {
        Some(row) => row,
        None => return None,
    };
    let id: i64 = row.get("id");
    let capacity: i64 = row.get("capacity");
    let data: Vec<u8> = row.get("data");
    let lock_code_hash: Option<Vec<u8>> = row.get("lock_code_hash");
    let lock_hash_type: Option<i16> = row.get("lock_hash_type");
    let lock_args: Option<Vec<u8>> = row.get("lock_args");
    let type_code_hash: Option<Vec<u8>> = row.get("type_code_hash");
    let type_hash_type: Option<i16> = row.get("type_hash_type");
    let type_args: Option<Vec<u8>> = row.get("type_args");

    let mut lock_builder = ScriptBuilder::default();
    if let Some(lock_code_hash) = lock_code_hash {
        lock_builder = lock_builder.code_hash(to_fixed_array::<32>(&lock_code_hash[0..32]).pack());
    }
    if let Some(lock_args) = lock_args {
        lock_builder = lock_builder.args(lock_args.pack());
    }
    if let Some(lock_hash_type) = lock_hash_type {
        lock_builder = lock_builder.hash_type(Byte::new(lock_hash_type as u8));
    }
    let lock_script = lock_builder.build();

    let mut type_builder = ScriptBuilder::default();
    if let Some(type_code_hash) = type_code_hash {
        type_builder = type_builder.code_hash(to_fixed_array::<32>(&type_code_hash[0..32]).pack());
    }
    if let Some(type_args) = type_args {
        type_builder = type_builder.args(type_args.pack());
    }
    if let Some(type_hash_type) = type_hash_type {
        type_builder = type_builder.hash_type(Byte::new(type_hash_type as u8));
    }
    let type_script = type_builder.build();

    let cell_output = CellOutput::new_builder()
        .capacity((capacity as u64).pack())
        .lock(lock_script)
        .type_(Some(type_script).pack())
        .build();

    Some((id, cell_output, data.into()))
}

async fn bulk_insert(
    table: &str,
    fields: &[&str],
    rows: &[Vec<FieldValue>],
    conflict_do_nothing_fields: Option<&[&str]>,
    tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    for bulk in rows.chunks(BATCH_SIZE_THRESHOLD) {
        // build query str
        let mut sql = build_bulk_insert_sql(table, fields, bulk)?;
        if let Some(fields) = conflict_do_nothing_fields {
            sql = format!("{} ON CONFLICT ({}) DO NOTHING", sql, fields.join(", "));
        }

        // bind
        let mut query = SQLXPool::new_query(&sql);
        for row in bulk {
            for field in row {
                query = field.bind(query);
            }
        }

        // execute
        query
            .execute(tx.as_mut())
            .await
            .map_err(|err| Error::DB(err.to_string()))?;
    }
    Ok(())
}

async fn bulk_insert_and_return_ids(
    table: &str,
    fields: &[&str],
    rows: &[Vec<FieldValue>],
    tx: &mut Transaction<'_, Any>,
) -> Result<Vec<i64>, Error> {
    let mut id_list = Vec::new();
    for bulk in rows.chunks(BATCH_SIZE_THRESHOLD) {
        // build query str
        let sql = build_bulk_insert_sql(table, fields, bulk)?;
        let sql = format!("{} RETURNING id", sql);

        // bind
        let mut query = SQLXPool::new_query(&sql);
        for row in bulk {
            for field in row {
                query = field.bind(query);
            }
        }

        // execute
        let mut rows = query
            .fetch_all(tx.as_mut())
            .await
            .map_err(|err| Error::DB(err.to_string()))?;
        id_list.append(&mut rows);
    }
    let ret: Vec<_> = id_list.iter().map(|row| row.get::<i64, _>("id")).collect();
    Ok(ret)
}

fn build_bulk_insert_sql(
    table: &str,
    fields: &[&str],
    bulk: &[Vec<FieldValue>],
) -> Result<String, Error> {
    let mut builder = SqlBuilder::insert_into(table);
    builder.fields(fields);
    bulk.iter().enumerate().for_each(|(row_index, row)| {
        let placeholders = (1..=row.len())
            .map(|i| format!("${}", i + row_index * row.len()))
            .collect::<Vec<String>>();
        builder.values(&placeholders);
    });
    let sql = builder
        .sql()
        .map_err(|err| Error::DB(err.to_string()))?
        .trim_end_matches(';')
        .to_string();
    Ok(sql)
}

fn block_view_to_field_values(block_view: &BlockView) -> Vec<FieldValue> {
    vec![
        block_view.hash().raw_data().to_vec().into(),
        (block_view.number() as i64).into(),
    ]
}
