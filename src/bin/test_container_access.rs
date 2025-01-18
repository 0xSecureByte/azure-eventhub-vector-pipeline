use std::sync::Arc;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use azure_identity::DefaultAzureCredential;
use tracing::{info, error};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    // Configuration
    let storage_account = "chkpnt";
    let container_name = "chkpnt-container";

    info!("Testing connection to Azure Blob Storage");
    info!("Storage Account: {}", storage_account);
    info!("Container: {}", container_name);

    // Create credentials and client
    let credential = Arc::new(DefaultAzureCredential::default());
    info!("Created Azure credential");
    
    let storage_credentials = StorageCredentials::token_credential(credential);
    info!("Created storage credentials");
    
    let service_client = BlobServiceClient::new(
        storage_account,
        storage_credentials,
    );
    info!("Created blob service client for account: {}", storage_account);

    let container_client = service_client.container_client(container_name);
    info!("Created container client for: {}", container_name);

    // First try to get properties
    match container_client.get_properties().await {
        Ok(_) => {
            info!("✅ Successfully connected to existing container");
            Ok(())
        }
        Err(e) => {
            error!("Failed to get container properties: {}", e);
            
            // If container doesn't exist, try to create it
            if e.to_string().contains("ContainerNotFound") {
                info!("Container not found, attempting to create it...");
                match container_client.create().await {
                    Ok(_) => {
                        info!("✅ Successfully created container");
                        Ok(())
                    }
                    Err(create_err) => {
                        error!("❌ Failed to create container: {}", create_err);
                        error!("Error type: {:?}", create_err);
                        Err(anyhow::anyhow!("Container creation failed: {}", create_err))
                    }
                }
            } else {
                error!("❌ Unexpected error accessing container");
                error!("Error type: {:?}", e);
                Err(anyhow::anyhow!("Container access failed: {}", e))
            }
        }
    }
}
