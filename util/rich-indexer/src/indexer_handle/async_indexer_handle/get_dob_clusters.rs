use super::*;

use crate::indexer::to_fixed_array;
use crate::store::SQLXPool;

use ckb_indexer_sync::Error;
use ckb_jsonrpc_types::{IndexerDobCluster, IndexerOrder, IndexerPagination, JsonBytes, Uint32};
use ckb_types::packed::OutPointBuilder;
use ckb_types::prelude::*;
use sql_builder::SqlBuilder;
use sqlx::{any::AnyRow, Row};

impl AsyncRichIndexerHandle {
    /// Get dob cells
    pub async fn get_dob_cluters(
        &self,
        order: IndexerOrder,
        limit: Uint32,
        after: Option<JsonBytes>,
    ) -> Result<IndexerPagination<IndexerDobCluster>, Error> {
        let limit = limit.value();
        if limit == 0 {
            return Err(Error::invalid_params("limit should be greater than 0"));
        }

        // query dob output
        let mut query_builder = SqlBuilder::select_from("cluster");
        query_builder
            .left()
            .join("cluster_output")
            .on("cluster.cluster_id = cluster_output.cluster_id");

        query_builder
            .field("cluster.cluster_id")
            .field("cluster.name")
            .field("cluster.description")
            .field("cluster_output.tx_id")
            .field("cluster_output.output_index");

        // filter cells in pool
        if let Some(after) = after {
            let after = decode_i64(after.as_bytes())?;
            match order {
                IndexerOrder::Asc => query_builder.and_where_gt("output.id", after),
                IndexerOrder::Desc => query_builder.and_where_lt("output.id", after),
            };
        }

        // sql string
        let sql = query_builder
            .sql()
            .map_err(|err| Error::DB(err.to_string()))?
            .trim_end_matches(';')
            .to_string();

        // bind
        let query = SQLXPool::new_query(&sql);

        // fetch
        let mut last_cursor = Vec::new();
        let clusters = self
            .store
            .fetch_all(query)
            .await
            .map_err(|err| Error::DB(err.to_string()))?
            .iter()
            .map(|row| {
                last_cursor = row.get::<i64, _>("cluster_id").to_le_bytes().to_vec();
                build_indexer_cluster(row)
            })
            .collect::<Vec<_>>();

        Ok(IndexerPagination {
            objects: clusters,
            last_cursor: JsonBytes::from_vec(last_cursor),
        })
    }
}

fn build_indexer_cluster(row: &AnyRow) -> IndexerDobCluster {
    IndexerDobCluster {
        id: JsonBytes::from_vec(row.get::<Vec<u8>, _>("cluster_id").to_vec()),
        name: JsonBytes::from_vec(row.get::<Vec<u8>, _>("name").to_vec()),
        description: JsonBytes::from_vec(row.get::<Vec<u8>, _>("description").to_vec()),
        out_point: OutPointBuilder::default()
            .tx_hash(to_fixed_array::<32>(&row.get::<Vec<u8>, _>("tx_id")).pack())
            .index((row.get::<i32, _>("output_index") as u32).pack())
            .build()
            .into(),
    }
}
