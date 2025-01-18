use anyhow::Result;
use azeventhubs::consumer::{EventHubConsumerClient, EventHubConsumerClientOptions, EventPosition, ReadEventOptions};
use azeventhubs::BasicRetryPolicy;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};
use futures::stream::StreamExt;
use crate::checkpoints::CheckpointStore;
use azure_identity::DefaultAzureCredential;


#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    pub max_batch_size: usize,
    pub partition_count: usize,
    pub buffer_size: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            partition_count: 4,
            buffer_size: 10000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub data: Vec<u8>,
    pub partition_id: String,
    pub sequence_number: i64,
    pub offset: i64,
}

pub struct EventHubConsumer {
    client: EventHubConsumerClient<BasicRetryPolicy>,
    fully_qualified_namespace: String,
    event_hub_name: String,
    consumer_group: String,
    config: ConsumerConfig,
    sender: mpsc::Sender<Event>,
    checkpoint_store: Option<Arc<CheckpointStore>>,
}

impl EventHubConsumer {
    pub fn new(
        client: EventHubConsumerClient<BasicRetryPolicy>,
        fully_qualified_namespace: String,
        event_hub_name: String,
        consumer_group: String,
        config: ConsumerConfig,
        checkpoint_store: Option<Arc<CheckpointStore>>,
    ) -> (Self, mpsc::Receiver<Event>) {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        
        (Self {
            client,
            fully_qualified_namespace,
            event_hub_name,
            consumer_group,
            config,
            sender,
            checkpoint_store,
        }, receiver)
    }

    pub async fn start_consuming(&mut self) -> Result<()> {
        let partition_ids = self.client.get_partition_ids().await?;
        info!("Found {} partitions: {:?}", partition_ids.len(), partition_ids);
        
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let checkpoint_store = self.checkpoint_store.clone();
        let sender = self.sender.clone();
        let max_batch_size = self.config.max_batch_size;
        let fully_qualified_namespace = self.fully_qualified_namespace.clone();
        let event_hub_name = self.event_hub_name.clone();
        let consumer_group = self.consumer_group.clone();

        for partition_id in partition_ids {
            let partition_checkpoint_store = checkpoint_store.clone();
            let sender = sender.clone();
            let partition_id = partition_id.clone();
            let fully_qualified_namespace = fully_qualified_namespace.clone();
            let event_hub_name = event_hub_name.clone();
            let consumer_group = consumer_group.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let credential = DefaultAzureCredential::default();
                    let mut client = EventHubConsumerClient::new_from_credential(
                        consumer_group.clone(),
                        fully_qualified_namespace.clone(),
                        event_hub_name.clone(),
                        credential,
                        EventHubConsumerClientOptions::default(),
                    ).await.unwrap_or_else(|e| {
                        error!("Failed to create client for partition {}: {}", partition_id, e);
                        panic!("Failed to create client");
                    });

                    let checkpoint_position = if let Some(store) = &partition_checkpoint_store {
                        match store.load_checkpoint(&partition_id).await {
                            Ok(Some(checkpoint)) => {
                                info!("Resuming from checkpoint: offset {} for partition {}", 
                                      checkpoint.offset, partition_id);
                                EventPosition::from_offset(checkpoint.offset, true)
                            }
                            _ => EventPosition::earliest(),
                        }
                    } else {
                        EventPosition::earliest()
                    };

                    let read_options = ReadEventOptions::default()
                        .with_prefetch_count(max_batch_size as u32);

                    let mut stream = match client.read_events_from_partition(
                        &partition_id,
                        checkpoint_position,
                        read_options
                    ).await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to create stream for partition {}: {}", partition_id, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    let mut last_checkpoint_time = tokio::time::Instant::now();
                    
                    while let Some(event_result) = stream.next().await {
                        match event_result {
                            Ok(event_data) => {
                                let sequence_number = event_data.sequence_number();
                                let offset = event_data.offset().unwrap_or_default();

                                let body = match event_data.body() {
                                    Ok(data) => {
                                        let body_vec = data.to_vec();
                                        body_vec
                                    },
                                    Err(e) => {
                                        error!("Failed to get event body from partition {}: {}", partition_id, e);
                                        continue;
                                    }
                                };

                                let processed_event = Event {
                                    data: body,
                                    partition_id: partition_id.clone(),
                                    sequence_number,
                                    offset,
                                };

                                match sender.send(processed_event).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!("Failed to send event from partition {} to channel: {}", partition_id, e);
                                        continue;
                                    }
                                }

                                // Checkpoint periodically
                                if let Some(store) = &partition_checkpoint_store {
                                    let now = tokio::time::Instant::now();
                                    if now.duration_since(last_checkpoint_time).as_secs() >= 30 {
                                        if let Err(e) = store.save_checkpoint(
                                            &partition_id,
                                            offset,
                                            sequence_number
                                        ).await {
                                            error!("Failed to save checkpoint for partition {}: {}", partition_id, e);
                                        }
                                        last_checkpoint_time = now;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error receiving event from partition {}: {}", partition_id, e);
                                continue;
                            }
                        }
                    }
                    
                    info!("Stream ended for partition {}, attempting to reconnect...", partition_id);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            });
            
            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                error!("Task error: {}", e);
            }
        }

        Ok(())
    }
}