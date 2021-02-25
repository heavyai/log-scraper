/*
 * Copyright 2021 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use log::{warn, debug};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Message}; // Headers
use rdkafka::topic_partition_list::TopicPartitionList;
// use rdkafka::util::get_rdkafka_version;

use crate::log_parser::{LogLine, LogWriter, new_log_writer, SResult, OutputType};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!("pre_rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("post_rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        // debug!("commit_callback: {:?}", result);
    }
}

type LogConsumer = StreamConsumer<CustomContext>;

pub enum InputFormat {
    Guess,
    Line,
    FilebeatJson,
}

async fn consume_logs(consumer: &LogConsumer, writer: &mut Box<dyn LogWriter>, hostname: Option<&str>) {
    let mut input_format = InputFormat::Guess;

    let mut hostname = match hostname{
        None => None,
        Some(hostname) => Some(hostname.to_string()),
    };

    let mut prev: Option<LogLine> = None;

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => continue,
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        continue
                    }
                };
                // info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                //       m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                // if let Some(headers) = m.headers() {
                //     for i in 0..headers.count() {
                //         let header = headers.get(i).unwrap();
                //         info!("  Header {:#?}: {:?}", header.0, header.1);
                //     }
                // }

                if let InputFormat::Guess = &input_format {
                    if payload.starts_with("{") {
                        input_format = InputFormat::FilebeatJson;
                    }
                    else {
                        input_format = InputFormat::Line;
                    }
                }

                let mut message_string = String::new();
                let message = match &input_format {
                    InputFormat::Guess => payload,
                    InputFormat::Line => payload,
                    InputFormat::FilebeatJson => {
                        match serde_json::from_str::<serde_json::Value>(payload) {
                            Ok(v) => {
                                if v.is_object() {
                                    if let Some(fields) = v.get("fields") {
                                        hostname = Some(fields["hostname"].to_string());
                                    }
                                    // TODO this is ugly, but is there a better way to get a &str?
                                    message_string.push_str(v["message"].as_str().unwrap());
                                    message_string.as_str()
                                }
                                else {
                                    payload
                                }
                            },
                            Err(_) => payload,
                        }
                    },
                };

                match LogLine::new(&message) {
                    Err(_) => {
                        match &prev {
                            None => {
                                warn!("Unexpected, unable to parse message: {:?}", m);
                                consumer.commit_message(&m, CommitMode::Async).unwrap();
                            },
                            Some(prev_x) => {
                                // TODO Is there a better way to append_msg without cloning that is allowed by the borrow checker?
                                let mut tmp = prev_x.clone();
                                tmp.append_msg(payload);
                                prev = Some(tmp);
                            },
                        }
                    },
                    Ok(curr) => {
                        match &prev {
                            None => {
                                prev = Some(curr);
                            },
                            Some(prev_x) => {
                                let mut tmp = prev_x.clone();
                                tmp.parse_msg();
                                tmp.hostname = hostname.clone();
                                match writer.write(&tmp) {
                                    Err(e) => warn!("Error writing {:?}", e),
                                    Ok(_) => {
                                        consumer.commit_message(&m, CommitMode::Async).unwrap()
                                    },
                                };

                                prev = Some(curr);
                            },
                        }
                    },
                }
            }
        };
    }
}

pub async fn consume_logs_main(brokers: &str, group_id: &str, topics: &[&str],
    output: Option<&str>, output_type: &OutputType, db: Option<&str>, hostname: Option<&str>)
-> SResult<()> {
    let context = CustomContext;

    // https://docs.rs/rdkafka/0.25.0/rdkafka/
    // https://github.com/edenhill/librdkafka/wiki
    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    let consumer: LogConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    // let filter: Vec<&str> = vec!();
    let mut writer = new_log_writer(None, &vec!(), output, &output_type, db)?;

    consume_logs(&consumer, &mut writer, hostname).await;

    writer.close()
}
