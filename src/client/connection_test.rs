use crate::{
    client::{BrokerConnection, ClientConfig, CreateTopic},
    proto::apis::{
        create_topics::CreateTopicsRequestV1,
        metadata::{MetadataRequestV2, MetadataResponseV2},
    },
    test_utils::init_logger,
    Error,
};
use log::info;

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

    let metadata: MetadataResponseV2 = c.exchange(&MetadataRequestV2 {
        topics: vec![String::from("test-topic")],
    })?;
    info!("Metadata:\n{:#?}", metadata);

    Ok(())
}

#[test]
fn create_topic() -> Result<(), Error> {
    init_logger();
    let mut c = connect()?;

    let request = CreateTopicsRequestV1 {
        topics: vec![CreateTopic::with_name("rskafka-create-topic-test")
            .partitions(2)
            .replication_factor(1)],
        timeout_ms: 1000,
        validate_only: false,
    };
    let response = c.exchange(&request)?;

    info!("Response:\n{:#?}", response);

    Ok(())
}
