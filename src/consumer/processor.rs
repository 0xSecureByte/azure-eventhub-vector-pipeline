use anyhow::Result;
use azeventhubs::consumer::{EventHubConsumerClient, EventPosition, ReadEventOptions};
use azeventhubs::BasicRetryPolicy;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};
use futures::stream::StreamExt;  // Changed to explicit import
use tokio::sync::Mutex;
use crate::checkpoints::CheckpointStore;


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
    client: Arc<Mutex<EventHubConsumerClient<BasicRetryPolicy>>>,
    config: ConsumerConfig,
    sender: mpsc::Sender<Event>,
    checkpoint_store: Option<Arc<CheckpointStore>>,
}

impl EventHubConsumer {
    pub fn new(
        client: Arc<Mutex<EventHubConsumerClient<BasicRetryPolicy>>>,
        config: ConsumerConfig,
        checkpoint_store: Option<Arc<CheckpointStore>>,
    ) -> (Self, mpsc::Receiver<Event>) {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        
        (Self {
            client,
            config,
            sender,
            checkpoint_store,
        }, receiver)
    }

    pub async fn start_consuming(&self) -> Result<()> {
        let partition_ids = self.get_partition_ids().await?;
        
        for partition_id in partition_ids {
            self.start_partition_consumer(partition_id).await?;
        }

        Ok(())
    }

    async fn get_partition_ids(&self) -> Result<Vec<String>> {
        let mut partition_ids = Vec::new();
        for i in 0..self.config.partition_count {
            partition_ids.push(i.to_string());
        }
        Ok(partition_ids)
    }

    async fn start_partition_consumer(&self, partition_id: String) -> Result<()> {
        let client = Arc::clone(&self.client);
        let sender = self.sender.clone();
        let checkpoint_store = self.checkpoint_store.clone();
        let _max_batch_size = self.config.max_batch_size;
    
        tokio::spawn(async move {
            info!("Starting consumer for partition {}", partition_id);
    
            let mut locked_client = client.lock().await;
    
            // Load last checkpoint if available
            let start_position = if let Some(store) = &checkpoint_store {
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
    
            let stream = locked_client
                .read_events_from_partition(
                    &partition_id,
                    start_position,
                    ReadEventOptions::default()
                )
                .await
                .map_err(|e| {
                    error!("Failed to create event stream: {}", e);
                    e
                })?;
    
            let mut stream = stream;
            let mut last_checkpoint_time = tokio::time::Instant::now();
    
            while let Some(event_result) = stream.next().await {
                match event_result {
                    Ok(event_data) => {
                        let body = match event_data.body() {
                            Ok(data) => {
                                let body_vec = data.to_vec();
                                // Log the actual event body
                                tracing::info!("Received event body: {:?}", String::from_utf8_lossy(&body_vec));
                                body_vec
                            },
                            Err(e) => {
                                error!("Failed to get event body: {}", e);
                                continue;
                            }
                        };
                        let sequence_number = event_data.sequence_number();
                        
                        let processed_event = Event {
                            data: body,
                            partition_id: partition_id.clone(),
                            sequence_number,
                            offset: event_data.offset().unwrap_or_default(),
                        };
    
                        if let Err(e) = sender.send(processed_event).await {
                            error!("Failed to send event to channel: {}", e);
                            break;
                        }
    
                        // Checkpoint periodically
                        if let Some(store) = &checkpoint_store {
                            let now = tokio::time::Instant::now();
                            if now.duration_since(last_checkpoint_time).as_secs() >= 30 {
                                if let Err(e) = store.save_checkpoint(
                                    &partition_id,
                                    event_data.offset().unwrap_or_default(),
                                    event_data.sequence_number(),
                                ).await {
                                    error!("Failed to save checkpoint: {}", e);
                                }
                                last_checkpoint_time = now;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error receiving event: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
    
            Ok::<(), anyhow::Error>(())
        });
    
        Ok(())
    
    }
}