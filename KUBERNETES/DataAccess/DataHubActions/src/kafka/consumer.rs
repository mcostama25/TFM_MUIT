use std::sync::Arc;

use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use tracing::{error, info, warn};

use crate::avro::AvroDecoder;
use crate::config::KafkaConfig;
use crate::error::AppError;
use crate::kafka::handler::handle_event;

/// Build a Kafka `StreamConsumer` from the given config.
/// Does NOT subscribe to topics yet — call `run_consumer_loop` for that.
pub fn build_consumer(cfg: &KafkaConfig) -> Result<StreamConsumer, AppError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap_servers)
        .set("group.id", &cfg.group_id)
        .set("auto.offset.reset", &cfg.auto_offset_reset)
        .set("enable.auto.commit", "true")
        .set("enable.partition.eof", "false")
        // Reduce log noise from librdkafka itself
        .set("log_level", "3")
        .create()?;

    Ok(consumer)
}

/// Subscribe to all configured topics and start consuming events indefinitely.
///
/// Errors on individual messages are logged and skipped — the loop never aborts
/// due to a single bad message. Hard Kafka infrastructure errors are propagated.
pub async fn run_consumer_loop(
    consumer: StreamConsumer,
    topics: &[String],
    decoder: Arc<AvroDecoder>,
) -> Result<(), AppError> {
    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topic_refs)?;
    info!(topics = ?topics, "Subscribed to Kafka topics");

    let mut stream = consumer.stream();

    loop {
        match stream.next().await {
            Some(Ok(msg)) => {
                let topic = msg.topic().to_owned();

                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        warn!(topic, "Received Kafka message with empty payload — skipping");
                        continue;
                    }
                };

                match decoder.decode(payload).await {
                    Ok(avro_val) => {
                        handle_event(&topic, avro_val);
                    }
                    Err(e) => {
                        warn!(topic, error = %e, "Failed to decode Avro message — skipping");
                    }
                }
            }
            Some(Err(e)) => {
                // Kafka consumer errors (e.g. partition rebalance, connectivity) — log but continue
                error!(error = %e, "Kafka consumer error");
            }
            None => {
                // Stream ended (consumer was closed)
                info!("Kafka consumer stream ended");
                break;
            }
        }
    }

    Ok(())
}
