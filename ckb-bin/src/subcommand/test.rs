use ckb_app_config::ExitCode;
use ckb_rich_indexer::indexer::cluster_cell_data::ClusterCellData;
use ckb_types::prelude::Entity;

pub fn test() -> Result<(), ExitCode> {
    let data = hex::decode("4c000000100000002b0000004c000000170000007465737420726762707020636f6c6c656374696f6e20311d00000074686973206973207465737420726762707020636f6c6c656374696f6e");
    let reader = ClusterCellData::from_slice(data.unwrap().as_slice()).unwrap();
    println!("{:?}", reader.name());
    Ok(())
}
