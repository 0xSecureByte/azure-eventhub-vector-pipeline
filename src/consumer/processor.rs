use anyhow::Result;
use azeventhubs::consumer::{EventHubConsumerClient, EventPosition, ReadEventOptions};
use azeventhubs::BasicRetryPolicy;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};
use futures::stream::StreamExt;  // Changed to explicit import
use tokio::sync::Mutex;


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
    pub offset: String,
}

pub struct EventHubConsumer {
    client: Arc<Mutex<EventHubConsumerClient<BasicRetryPolicy>>>,
    config: ConsumerConfig,
    sender: mpsc::Sender<Event>,
}

impl EventHubConsumer {
    pub fn new(
        client: Arc<Mutex<EventHubConsumerClient<BasicRetryPolicy>>>,
        config: ConsumerConfig,
    ) -> (Self, mpsc::Receiver<Event>) {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        
        (Self {
            client,
            config,
            sender,
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
        let _max_batch_size = self.config.max_batch_size;
    
        tokio::spawn(async move {
            info!("Starting consumer for partition {}", partition_id);
    
            let mut locked_client = client.lock().await;
    
            let stream = locked_client
                .read_events_from_partition(
                    &partition_id,
                    EventPosition::latest(),
                    ReadEventOptions::default()
                )
                .await
                .map_err(|e| {
                    error!("Failed to create event stream: {}", e);
                    e
                })?;
    
            let mut stream = stream;
    
            while let Some(event_result) = stream.next().await {
                match event_result {
                    Ok(event_data) => {
                        let body = match event_data.body() {
                            Ok(data) => data.to_vec(),
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
                            offset: event_data.offset()
                                .map(|o| o.to_string())
                                .unwrap_or_else(|| "0".to_string()),
                        };
    
                        if let Err(e) = sender.send(processed_event).await {
                            error!("Failed to send event to channel: {}", e);
                            break;
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