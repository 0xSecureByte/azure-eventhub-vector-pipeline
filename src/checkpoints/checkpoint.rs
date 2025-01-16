    use anyhow::{Result, Context};
    use azure_storage::StorageCredentials;
    use azure_storage_blobs::prelude::*;
    use azure_identity::DefaultAzureCredential;
    use serde::{Serialize, Deserialize};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing::{info, warn, error};
    use backoff::{ExponentialBackoff, Error as BackoffError};
    use futures::StreamExt;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Checkpoint {
        pub partition_id: String,
        pub offset: i64,
        pub sequence_number: i64,
        pub updated_time: i64,
    }

    pub struct CheckpointStore {
        container_client: ContainerClient,
        checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
        event_hub_name: String,
    }

    impl CheckpointStore {
        pub async fn new(
            storage_account: &str,
            container_name: &str,
            event_hub_name: &str,
        ) -> Result<Self> {
            // Validate inputs
            if storage_account.is_empty() {
                return Err(anyhow::anyhow!("Storage account name cannot be empty"));
            }
            if container_name.is_empty() {
                return Err(anyhow::anyhow!("Container name cannot be empty"));
            }

            let credential = Arc::new(DefaultAzureCredential::default());
            let storage_credentials = StorageCredentials::token_credential(credential);
            
            // Create the service client with full URL
            let service_client = BlobServiceClient::new(
                &format!("{}.blob.core.windows.net", storage_account),
                storage_credentials,
            );

            // Get container client
            let container_client = service_client.container_client(container_name);

            // Retry container creation with backoff
            let backoff = ExponentialBackoff {
                max_elapsed_time: Some(std::time::Duration::from_secs(30)),
                max_interval: std::time::Duration::from_secs(5),
                ..ExponentialBackoff::default()
            };

            let result = backoff::future::retry(backoff, || async {
                match container_client.create().await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        if e.to_string().contains("AuthorizationFailure") {
                            error!("Authorization failure creating container. Please check Azure role assignments: {}", e);
                            Err(BackoffError::permanent(e))
                        } else {
                            error!("Failed to create container, will retry: {}", e);
                            Err(BackoffError::transient(e))
                        }
                    }
                }
            }).await;

            match result {
                Ok(_) => {
                    info!("Successfully created/connected to container: {}", container_name);
                    Ok(Self {
                        container_client,
                        checkpoints: Arc::new(RwLock::new(HashMap::new())),
                        event_hub_name: event_hub_name.to_string(),
                    })
                }
                Err(e) => Err(anyhow::anyhow!("Failed to create/connect to container after retries: {}", e)),
            }
        }

        pub async fn save_checkpoint(
            &self,
            partition_id: &str,
            offset: i64,
            sequence_number: i64,
        ) -> Result<()> {
            let checkpoint = Checkpoint {
                partition_id: partition_id.to_string(),
                offset,
                sequence_number,
                updated_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
            };

            // Save to memory
            {
                let mut checkpoints = self.checkpoints.write().await;
                checkpoints.insert(partition_id.to_string(), checkpoint.clone());
            }

            // Save to blob storage
            let blob_name = format!("{}/{}/checkpoint.json", self.event_hub_name, partition_id);
            let blob = self.container_client
                .blob_client(&blob_name);

            let content = serde_json::to_vec(&checkpoint)
                .context("Failed to serialize checkpoint")?;

            blob.put_block_blob(content)
                .content_type("application/json")
                .await
                .map_err(|e| {
                    warn!("Failed to save checkpoint to storage: {}", e);
                    anyhow::anyhow!("Failed to save checkpoint: {}", e)
                })?;

            info!("Saved checkpoint for partition {}", partition_id);
            Ok(())
        }

        pub async fn load_checkpoint(&self, partition_id: &str) -> Result<Option<Checkpoint>> {
            // Try memory first
            {
                let checkpoints = self.checkpoints.read().await;
                if let Some(checkpoint) = checkpoints.get(partition_id) {
                    return Ok(Some(checkpoint.clone()));
                }
            }

            // Try blob storage
            let blob_name = format!("{}/{}/checkpoint.json", self.event_hub_name, partition_id);
            let blob = self.container_client
                .blob_client(&blob_name);

            match blob.get().into_stream().next().await {
                Some(Ok(response)) => {
                    let mut data = Vec::new();
                    let mut stream = response.data;
                    
                    while let Some(chunk) = stream.next().await {
                        data.extend_from_slice(&chunk?);
                    }
                    
                    let checkpoint: Checkpoint = serde_json::from_slice(&data)
                        .context("Failed to deserialize checkpoint")?;
                    
                    // Cache in memory
                    let mut checkpoints = self.checkpoints.write().await;
                    checkpoints.insert(partition_id.to_string(), checkpoint.clone());
                    
                    Ok(Some(checkpoint))
                }
                _ => Ok(None),
            }
        }
    }