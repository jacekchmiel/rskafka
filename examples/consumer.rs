use anyhow::Error;
use futures::prelude::*;
use rskafka::{Consumer, ConsumerConfig};
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::Builder::new()
        .parse_filters("rskafka::consumer=trace,rskafka::fetch=trace,info") //
        .init();

    let config = ConsumerConfig {
        client_id: "rskafka-example".into(),
        group_id: "rskafka-example".into(),
        topics: vec!["rskafka-test".into()],
    };

    let consumer = Consumer::bootstrap("localhost:9092", config).await?;
    let (killswitch, assignment_stream) = consumer.split();

    // Register graceful shutdown procedure
    tokio::spawn(async {
        signal::ctrl_c().await?;
        killswitch.shutdown().await;

        Ok::<(), anyhow::Error>(())
    });

    let result = assignment_stream
        .try_for_each(|assignment| async move {
            println!("Assignment received");
            let committer = assignment.commit_sink();

            assignment
                .into_message_stream()
                .then(|msg| async move {
                    println!("{:#?}", msg);
                    msg.into_offset()
                })
                .map(Ok)
                .forward(committer)
                .await?; // Commit sink cannot really fail for now

            println!("Assignment revoked");
            Ok(())
        })
        .await
        .map_err(|e| e.0);

    match result {
        Ok(_) => println!("Finished"),
        Err(e) => eprintln!("{:?}", e),
    }

    Ok(())
}
