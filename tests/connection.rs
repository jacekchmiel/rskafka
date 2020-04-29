extern crate rskafka;

use log::info;
use rskafka::client::{BrokerConnection, ClientConfig, CreateTopic, Error};
use std::time::Duration;

fn init_logger() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init()
        .ok();
}

fn connect() -> Result<BrokerConnection, Error> {
    BrokerConnection::connect(
        ClientConfig::builder()
            .bootstrap_servers("localhost:9092".to_string())
            .build()?,
    )
}

#[test]
fn read_versions() -> Result<(), Error> {
    init_logger();
    let mut c = connect()?;

    let versions = c.get_api_versions()?;
    info!("Supported versions:\n{:#?}", versions);

    Ok(())
}

#[test]
fn read_metadata() -> Result<(), Error> {
    init_logger();
    let mut c = connect()?;

    let metadata = c.get_metadata(vec![String::from("test-topic")])?;
    info!("Metadata:\n{:#?}", metadata);

    Ok(())
}

#[test]
fn create_topic() -> Result<(), Error> {
    init_logger();
    let mut c = connect()?;

    let topic = CreateTopic::with_name("rskafka-create-topic-test")
        .partitions(2)
        .replication_factor(1);

    let created_topics = c.create_topics(vec![topic], Duration::from_secs(1), false)?;
    info!("Response:\n{:#?}", created_topics);

    Ok(())
}
