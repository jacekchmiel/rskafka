extern crate rskafka;

use log::info;
use rskafka::client::{ClientConfig, Error, SimpleClient};

#[test]
fn read_versions() -> Result<(), Error> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init()
        .ok();

    info!("read_versions");

    let mut client = SimpleClient::connect(
        ClientConfig::builder()
            .bootstrap_servers("localhost:9092".to_string())
            .build()?,
    )?;
    let versions = client.get_api_versions()?;
    info!("Supported versions:\n{:#?}", versions);

    Ok(())
}

#[test]
fn read_metadata() -> Result<(), Error> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init()
        .ok();

    info!("read_metadata");

    let mut client = SimpleClient::connect(
        ClientConfig::builder()
            .bootstrap_servers("localhost:9092".to_string())
            .build()?,
    )?;
    let metadata = client.get_metadata(vec![String::from("test-topic")])?;
    info!("Metadata:\n{:#?}", metadata);

    Ok(())
}
