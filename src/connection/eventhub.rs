use std::sync::Arc;
use anyhow::Result;
use azeventhubs::consumer::{EventHubConsumerClient, EventHubConsumerClientOptions};
use azeventhubs::{EventHubsRetryPolicy, BasicRetryPolicy, EventHubsRetryOptions};
use azure_identity::DefaultAzureCredential;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Clone, Debug)]
pub struct EventHubConfig {
    pub fully_qualified_namespace: String,
    pub event_hub_name: String,
    pub consumer_group: String,
}

pub struct EventHubConnection<RP = BasicRetryPolicy> 
where
    RP: EventHubsRetryPolicy + Send,
{
    config: EventHubConfig,
    client: Mutex<Option<Arc<EventHubConsumerClient<RP>>>>,
}

impl<RP> EventHubConnection<RP>
where
    RP: EventHubsRetryPolicy + Send + From<EventHubsRetryOptions>,
{
    pub fn new(config: EventHubConfig) -> Self {
        EventHubConnection {
            config,
            client: Mutex::new(None),
        }
    }

    pub async fn connect(&self) -> Result<()> {
        let credential = DefaultAzureCredential::default();
        let client_options = EventHubConsumerClientOptions::default();
        
        let client = EventHubConsumerClient::with_policy::<RP>()
            .new_from_credential(
                self.config.consumer_group.clone(),
                self.config.fully_qualified_namespace.clone(),
                self.config.event_hub_name.clone(),
                credential,
                client_options,
            ).await?;

        let mut locked_client = self.client.lock().await;
        *locked_client = Some(Arc::new(client));

        info!("Connected to Event Hub: {}", self.config.event_hub_name);
        Ok(())
    }

    pub async fn get_client(&self) -> Result<Arc<EventHubConsumerClient<RP>>> {
        let locked_client = self.client.lock().await;
        match &*locked_client {
            Some(client) => Ok(client.clone()),
            None => Err(anyhow::anyhow!("Client not connected")),
        }
    }
}