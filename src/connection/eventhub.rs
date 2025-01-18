use anyhow::Result;
use azeventhubs::consumer::{EventHubConsumerClient, EventHubConsumerClientOptions};
use azeventhubs::{EventHubsRetryPolicy, BasicRetryPolicy, EventHubsRetryOptions};
use azure_identity::DefaultAzureCredential;
use tokio::sync::Mutex;
use tracing::info;
use std::time::Duration;
use azeventhubs::MaxRetries;

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
    client: Mutex<Option<EventHubConsumerClient<RP>>>,
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
        info!("Attempting to connect to Event Hub: {}", self.config.event_hub_name);
        info!("Namespace: {}", self.config.fully_qualified_namespace);
        info!("Consumer Group: {}", self.config.consumer_group);
        
        let credential = DefaultAzureCredential::default();
        let mut client_options = EventHubConsumerClientOptions::default();
        
        // Remove operation_timeout and keep only valid options
        client_options.connection_options.connection_idle_timeout = Duration::from_secs(300);
        client_options.retry_options.max_retries = MaxRetries::new(5).unwrap();
        client_options.retry_options.delay = Duration::from_secs(2);
        
        let client = EventHubConsumerClient::with_policy::<RP>()
            .new_from_credential(
                self.config.consumer_group.clone(),
                self.config.fully_qualified_namespace.clone(),
                self.config.event_hub_name.clone(),
                credential,
                client_options,
            ).await?;

        let mut locked_client = self.client.lock().await;
        *locked_client = Some(client);

        info!("Connected to Event Hub: {}", self.config.event_hub_name);
        Ok(())
    }

    pub async fn get_client(&self) -> Result<(String, EventHubConsumerClient<RP>)> {
        let mut locked_client = self.client.lock().await;
        match locked_client.take() {
            Some(client) => Ok((self.config.event_hub_name.clone(), client)),
            None => Err(anyhow::anyhow!("Client not connected")),
        }
    }

    pub fn get_config(&self) -> &EventHubConfig {
        &self.config
    }
}